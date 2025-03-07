package main

import (
	"archive/tar"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	LargeFileThreshold = 100 * 1024 * 1024 // 100MB
	BufferSize         = 1 * 1024 * 1024   // 1MB
	ConcurrentThreads  = 4
)

type FastCopySystem struct {
	Src         string
	Dest        string
	IndexDB     *sql.DB
	WaitGroup   sync.WaitGroup
	ProgressChan chan int
}

func NewFastCopySystem(src, dest string) *FastCopySystem {
	db, err := sql.Open("sqlite3", "file_index.db")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS file_index (
		path TEXT PRIMARY KEY, 
		size INTEGER, 
		mtime DATETIME)`)
	if err != nil {
		log.Fatal(err)
	}
	return &FastCopySystem{
		Src:         src,
		Dest:        dest,
		IndexDB:     db,
		ProgressChan: make(chan int, 100),
	}
}

func (f *FastCopySystem) UpdateIndex() {
	filepath.Walk(f.Src, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			_, err = f.IndexDB.Exec(
				"REPLACE INTO file_index VALUES (?, ?, ?)",
				path, info.Size(), info.ModTime())
			if err != nil {
				log.Println("Index update error:", err)
			}
		}
		return nil
	})
}

func (f *FastCopySystem) ArchiveSmallFiles() int {
	var smallFiles []string
	rows, _ := f.IndexDB.Query(
		"SELECT path FROM file_index WHERE size < ?", 
		LargeFileThreshold)
	defer rows.Close()

	for rows.Next() {
		var path string
		rows.Scan(&path)
		smallFiles = append(smallFiles, path)
	}

	tarFile, _ := os.Create(filepath.Join(f.Dest, "_temp_archive.tar"))
	defer tarFile.Close()

	tw := tar.NewWriter(tarFile)
	defer tw.Close()

	for _, file := range smallFiles {
		fi, _ := os.Stat(file)
		header, _ := tar.FileInfoHeader(fi, "")
		header.Name = file
		tw.WriteHeader(header)

		f, _ := os.Open(file)
		io.Copy(tw, f)
		f.Close()
	}

	return len(smallFiles)
}

func (f *FastCopySystem) CopyLargeFile(src, dest string) {
	file, _ := os.Open(src)
	defer file.Close()

	stat, _ := file.Stat()
	chunkSize := stat.Size() / ConcurrentThreads

	destFile, _ := os.Create(dest)
	defer destFile.Close()

	var mutex sync.Mutex
	for i := 0; i < ConcurrentThreads; i++ {
		f.WaitGroup.Add(1)
		go func(threadNum int) {
			defer f.WaitGroup.Done()

			start := int64(threadNum) * chunkSize
			end := start + chunkSize
			if threadNum == ConcurrentThreads-1 {
				end = stat.Size()
			}

			buf := make([]byte, BufferSize)
			file.Seek(start, 0)
			destFile.Seek(start, 0)

			for pos := start; pos < end; {
				readSize := min(BufferSize, end-pos)
				n, _ := file.Read(buf[:readSize])

				mutex.Lock()
				destFile.Write(buf[:n])
				mutex.Unlock()

				pos += int64(n)
				f.ProgressChan <- n
			}
		}(i)
	}
}

func (f *FastCopySystem) Run() {
	start := time.Now()
	
	// 更新索引
	fmt.Println("[*] Updating file index...")
	f.UpdateIndex()

	// 归档小文件
	fmt.Println("[*] Archiving small files...")
	archiveCount := f.ArchiveSmallFiles()

	// 复制大文件
	fmt.Println("[*] Copying large files...")
	rows, _ := f.IndexDB.Query(
		"SELECT path FROM file_index WHERE size >= ?", 
		LargeFileThreshold)
	defer rows.Close()

	var largeFiles []string
	for rows.Next() {
		var path string
		rows.Scan(&path)
		largeFiles = append(largeFiles, path)
	}

	// 进度监控
	go func() {
		var total int64
		for n := range f.ProgressChan {
			total += int64(n)
			fmt.Printf("\rCopied: %.2f MB", float64(total)/1024/1024)
		}
	}()

	for _, path := range largeFiles {
		relPath, _ := filepath.Rel(f.Src, path)
		destPath := filepath.Join(f.Dest, relPath)
		os.MkdirAll(filepath.Dir(destPath), os.ModePerm)
		f.CopyLargeFile(path, destPath)
	}

	f.WaitGroup.Wait()
	close(f.ProgressChan)

	// 解压归档
	fmt.Println("\n[*] Extracting small files...")
	f.ExtractArchive()

	fmt.Printf("[+] Completed in %.2f seconds\n", time.Since(start).Seconds())
	fmt.Printf("    Archived files: %d\n", archiveCount)
	fmt.Printf("    Copied large files: %d\n", len(largeFiles))
}

func (f *FastCopySystem) ExtractArchive() {
	tarFile, _ := os.Open(filepath.Join(f.Dest, "_temp_archive.tar"))
	defer tarFile.Close()
	defer os.Remove(filepath.Join(f.Dest, "_temp_archive.tar"))

	tr := tar.NewReader(tarFile)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		target := filepath.Join(f.Dest, header.Name)
		os.MkdirAll(filepath.Dir(target), os.ModePerm)

		file, _ := os.Create(target)
		io.Copy(file, tr)
		file.Close()
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func main() {
	copier := NewFastCopySystem(
		"D:\\Minecraft",
		"E:\\Backup",
	)
	copier.Run()
}