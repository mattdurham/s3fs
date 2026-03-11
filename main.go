// s3fs is a minimal S3-compatible server backed by the local filesystem.
// It implements the subset of the S3 API that Tempo and blockpack actually use:
//
//   - PUT    /{bucket}                        → CreateBucket
//   - PUT    /{bucket}/{key...}               → PutObject
//   - GET    /{bucket}/{key...}               → GetObject (with Range header support)
//   - HEAD   /{bucket}/{key...}               → HeadObject (StatObject)
//   - DELETE /{bucket}/{key...}               → DeleteObject
//   - GET    /{bucket}?list-type=2            → ListObjectsV2
//   - POST   /{bucket}/{key...}?uploads       → InitiateMultipartUpload
//   - PUT    /{bucket}/{key...}?partNumber=N&uploadId=X → UploadPart
//   - POST   /{bucket}/{key...}?uploadId=X   → CompleteMultipartUpload
//
// Configuration via environment variables:
//
//	S3FS_ROOT     — data directory (default: ./data)
//	S3FS_PORT     — listen port (default: 8080)
//	S3FS_LATENCY  — artificial latency per request in milliseconds (default: 0)
package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	root    string
	latency time.Duration

	multipartMu sync.Mutex
	multiparts  = map[string]*multipartUpload{} // uploadID → upload
	nextUpload  int
)

type multipartUpload struct {
	bucket string
	key    string
	parts  map[int]string // partNumber → temp file path
}

func main() {
	root = env("S3FS_ROOT", "./data")
	port := env("S3FS_PORT", "8080")

	if ms := env("S3FS_LATENCY", "0"); ms != "0" {
		n, err := strconv.Atoi(ms)
		if err != nil {
			slog.Error("invalid S3FS_LATENCY", "value", ms, "err", err)
			os.Exit(1)
		}
		latency = time.Duration(n) * time.Millisecond
	}

	if err := os.MkdirAll(root, 0o755); err != nil {
		slog.Error("failed to create root directory", "path", root, "err", err)
		os.Exit(1)
	}

	slog.Info("s3fs starting", "root", root, "port", port, "latency", latency)
	if err := http.ListenAndServe(":"+port, http.HandlerFunc(handler)); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func handler(w http.ResponseWriter, r *http.Request) {
	if latency > 0 {
		time.Sleep(latency)
	}

	// Parse /{bucket} or /{bucket}/{key...}
	path := strings.TrimPrefix(r.URL.Path, "/")

	// Health check and ListBuckets: GET / (no bucket)
	if path == "" || path == "minio/health/live" || path == "minio/health/ready" {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "ok")
			return
		}
	}

	bucket, key, _ := strings.Cut(path, "/")

	if bucket == "" {
		http.Error(w, "missing bucket", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()

	switch {
	// HeadBucket: HEAD /{bucket} (minio connectivity check)
	case r.Method == http.MethodHead && key == "":
		headBucket(w, bucket)

	// ListObjectsV2: GET /{bucket}?list-type=2  or  GET /{bucket} (no key)
	case r.Method == http.MethodGet && key == "":
		listObjects(w, r, bucket, q.Get("prefix"), q.Get("delimiter"), q.Get("start-after"), q.Get("continuation-token"))

	// CreateBucket: PUT /{bucket} (no key)
	case r.Method == http.MethodPut && key == "":
		createBucket(w, bucket)

	// InitiateMultipartUpload: POST /{bucket}/{key}?uploads
	case r.Method == http.MethodPost && q.Has("uploads"):
		initiateMultipart(w, bucket, key)

	// UploadPart: PUT /{bucket}/{key}?partNumber=N&uploadId=X
	case r.Method == http.MethodPut && q.Get("uploadId") != "" && q.Get("partNumber") != "":
		uploadPart(w, r, q.Get("uploadId"), q.Get("partNumber"))

	// CompleteMultipartUpload: POST /{bucket}/{key}?uploadId=X
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		completeMultipart(w, bucket, key, q.Get("uploadId"), r)

	// PutObject: PUT /{bucket}/{key}
	case r.Method == http.MethodPut && key != "":
		putObject(w, r, bucket, key)

	// GetObject: GET /{bucket}/{key}
	case r.Method == http.MethodGet && key != "":
		getObject(w, r, bucket, key)

	// HeadObject: HEAD /{bucket}/{key}
	case r.Method == http.MethodHead && key != "":
		headObject(w, bucket, key)

	// DeleteObject: DELETE /{bucket}/{key}
	case r.Method == http.MethodDelete && key != "":
		deleteObject(w, bucket, key)

	default:
		slog.Warn("unhandled request", "method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery)
		http.Error(w, "not implemented", http.StatusNotImplemented)
	}
}

// --- Bucket operations ---

func createBucket(w http.ResponseWriter, bucket string) {
	dir := filepath.Join(root, bucket)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func headBucket(w http.ResponseWriter, bucket string) {
	dir := filepath.Join(root, bucket)
	if _, err := os.Stat(dir); err != nil {
		writeError(w, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- Object operations ---

func putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	fp := objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	f, err := os.Create(fp)
	if err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(io.MultiWriter(f, h), r.Body); err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	etag := hex.EncodeToString(h.Sum(nil))
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	fp := objectPath(bucket, key)
	info, err := os.Stat(fp)
	if err != nil {
		writeError(w, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		return
	}

	size := info.Size()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", makeETag(fp))
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		f, err := os.Open(fp)
		if err != nil {
			writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
			return
		}
		defer f.Close()
		io.Copy(w, f)
		return
	}

	// Parse Range: bytes=start-end
	start, end, ok := parseRange(rangeHeader, size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	length := end - start + 1
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))

	f, err := os.Open(fp)
	if err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	f.Seek(start, io.SeekStart)
	w.WriteHeader(http.StatusPartialContent)
	io.CopyN(w, f, length)
}

func headObject(w http.ResponseWriter, bucket, key string) {
	fp := objectPath(bucket, key)
	info, err := os.Stat(fp)
	if err != nil {
		writeError(w, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", makeETag(fp))
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func deleteObject(w http.ResponseWriter, bucket, key string) {
	fp := objectPath(bucket, key)
	os.Remove(fp)
	// S3 returns 204 even if the object didn't exist.
	w.WriteHeader(http.StatusNoContent)
}

// --- ListObjectsV2 ---

type listResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []listObject   `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
}

type listObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func listObjects(w http.ResponseWriter, _ *http.Request, bucket, prefix, delimiter, startAfter, contToken string) {
	bucketDir := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketDir); err != nil {
		writeError(w, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
		return
	}

	// Per S3 spec: if continuation-token is set, start-after is ignored.
	marker := contToken
	if marker == "" {
		marker = startAfter
	}

	var objects []listObject
	prefixSet := map[string]bool{}

	filepath.Walk(bucketDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(bucketDir, path)
		key := filepath.ToSlash(rel)

		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		// Skip keys at or before the marker (start-after or continuation-token).
		if marker != "" && key <= marker {
			return nil
		}

		// Handle delimiter (typically "/") for directory-like listing.
		if delimiter != "" {
			after := strings.TrimPrefix(key, prefix)
			if idx := strings.Index(after, delimiter); idx >= 0 {
				cp := prefix + after[:idx+len(delimiter)]
				if marker == "" || cp > marker {
					prefixSet[cp] = true
				}
				return nil
			}
		}

		objects = append(objects, listObject{
			Key:          key,
			LastModified: info.ModTime().UTC().Format(time.RFC3339),
			ETag:         makeETag(path),
			Size:         info.Size(),
			StorageClass: "STANDARD",
		})
		return nil
	})

	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })

	var cps []commonPrefix
	for p := range prefixSet {
		cps = append(cps, commonPrefix{Prefix: p})
	}
	sort.Slice(cps, func(i, j int) bool { return cps[i].Prefix < cps[j].Prefix })

	const maxKeys = 1000
	truncated := len(objects) > maxKeys
	nextToken := ""
	if truncated {
		nextToken = objects[maxKeys-1].Key
		objects = objects[:maxKeys]
	}

	result := listResult{
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		KeyCount:              len(objects),
		MaxKeys:               maxKeys,
		IsTruncated:           truncated,
		Contents:              objects,
		CommonPrefixes:        cps,
		NextContinuationToken: nextToken,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(result)
}

// --- Multipart Upload ---

type initiateResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

func initiateMultipart(w http.ResponseWriter, bucket, key string) {
	multipartMu.Lock()
	nextUpload++
	uploadID := fmt.Sprintf("upload-%d", nextUpload)
	multiparts[uploadID] = &multipartUpload{
		bucket: bucket,
		key:    key,
		parts:  make(map[int]string),
	}
	multipartMu.Unlock()

	result := initiateResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(result)
}

func uploadPart(w http.ResponseWriter, r *http.Request, uploadID, partNumStr string) {
	partNum, err := strconv.Atoi(partNumStr)
	if err != nil {
		writeError(w, "InvalidArgument", "Invalid partNumber", http.StatusBadRequest)
		return
	}

	multipartMu.Lock()
	up, ok := multiparts[uploadID]
	multipartMu.Unlock()
	if !ok {
		writeError(w, "NoSuchUpload", "Upload not found", http.StatusNotFound)
		return
	}

	tmpFile, err := os.CreateTemp("", "s3fs-part-*")
	if err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	h := md5.New()
	if _, err := io.Copy(io.MultiWriter(tmpFile, h), r.Body); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	tmpFile.Close()

	multipartMu.Lock()
	up.parts[partNum] = tmpFile.Name()
	multipartMu.Unlock()

	etag := hex.EncodeToString(h.Sum(nil))
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

type completeMultipartUploadRequest struct {
	Parts []completePart `xml:"Part"`
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func completeMultipart(w http.ResponseWriter, bucket, key, uploadID string, r *http.Request) {
	multipartMu.Lock()
	up, ok := multiparts[uploadID]
	if ok {
		delete(multiparts, uploadID)
	}
	multipartMu.Unlock()

	if !ok {
		writeError(w, "NoSuchUpload", "Upload not found", http.StatusNotFound)
		return
	}

	// Parse the completion request to get part order.
	var req completeMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "MalformedXML", err.Error(), http.StatusBadRequest)
		return
	}

	sort.Slice(req.Parts, func(i, j int) bool {
		return req.Parts[i].PartNumber < req.Parts[j].PartNumber
	})

	fp := objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}

	out, err := os.Create(fp)
	if err != nil {
		writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
		return
	}
	defer out.Close()

	h := md5.New()
	for _, p := range req.Parts {
		tmpPath, exists := up.parts[p.PartNumber]
		if !exists {
			writeError(w, "InvalidPart", fmt.Sprintf("part %d not found", p.PartNumber), http.StatusBadRequest)
			return
		}
		partFile, err := os.Open(tmpPath)
		if err != nil {
			writeError(w, "InternalError", err.Error(), http.StatusInternalServerError)
			return
		}
		io.Copy(io.MultiWriter(out, h), partFile)
		partFile.Close()
		os.Remove(tmpPath)
	}

	etag := hex.EncodeToString(h.Sum(nil))
	result := completeResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: fmt.Sprintf("/%s/%s", bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     `"` + etag + `"`,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(result)
}

// --- Helpers ---

func objectPath(bucket, key string) string {
	return filepath.Join(root, bucket, filepath.FromSlash(key))
}

func parseRange(header string, size int64) (start, end int64, ok bool) {
	// Range: bytes=start-end
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	if parts[0] == "" {
		// Suffix range: bytes=-N → last N bytes
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		start = size - n
		if start < 0 {
			start = 0
		}
		return start, size - 1, true
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}

	if parts[1] == "" {
		return start, size - 1, true
	}

	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	if end < start {
		return 0, 0, false
	}
	return start, end, true
}

func makeETag(fp string) string {
	info, err := os.Stat(fp)
	if err != nil {
		return `"0"`
	}
	// Fast ETag from path + size + mtime (avoids reading file).
	h := md5.New()
	fmt.Fprintf(h, "%s:%d:%d", fp, info.Size(), info.ModTime().UnixNano())
	return `"` + hex.EncodeToString(h.Sum(nil)) + `"`
}

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeError(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprint(w, xml.Header)
	xml.NewEncoder(w).Encode(s3Error{Code: code, Message: message})
}
