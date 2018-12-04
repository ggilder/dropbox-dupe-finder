package main

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/dustin/go-humanize/english"
	"github.com/jessevdk/go-flags"
	"github.com/tj/go-dropbox"
	"golang.org/x/text/unicode/norm"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
)

// File stores the result of either Dropbox API or local file listing
type File struct {
	Path string
	Size uint64
}

type DropboxManifest map[string][]*File

type scanProgressUpdate struct {
	Count             int
	RawDuplicateCount int
}

type Duplication struct {
	ContentHash    string
	Files          []*File
	DuplicateCount int
	DuplicateSize  uint64
}

type DuplicateReport struct {
	Duplications        []*Duplication
	TotalDuplicateCount int
	TotalDuplicateSize  uint64
}

func main() {
	token := os.Getenv("DROPBOX_ACCESS_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "Missing Dropbox OAuth token! Please set the DROPBOX_ACCESS_TOKEN environment variable.")
		os.Exit(1)
	}

	var opts struct {
		Verbose            bool   `short:"v" long:"verbose" description:"Show verbose debug information"`
		RemoteRoot         string `short:"r" long:"remote" description:"Directory in Dropbox to verify" default:""`
		FreeMemoryInterval int    `long:"free-memory-interval" description:"Interval (in seconds) to manually release unused memory back to the OS on low-memory systems" default:"0"`
	}

	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	// Dropbox API uses empty string for root, but for figuring out relative
	// paths of the returned entries it's easier to use "/". Conversion is
	// handled before the API call.
	remoteRoot := opts.RemoteRoot
	if remoteRoot[0] != '/' {
		remoteRoot = "/" + remoteRoot
	}

	dbxClient := dropbox.New(dropbox.NewConfig(token))

	fmt.Printf("Scanning Dropbox directory \"%v\" for duplicates\n\n", remoteRoot)

	progressChan := make(chan *scanProgressUpdate)
	var wg sync.WaitGroup
	wg.Add(1)

	var dropboxManifest DropboxManifest
	var dropboxErr error
	go func() {
		dropboxManifest, dropboxErr = getDropboxManifest(progressChan, dbxClient, remoteRoot)
		wg.Done()
	}()

	go func() {
		for update := range progressChan {
			if opts.Verbose {
				// TODO decide if raw duplicate count is actually useful
				fmt.Fprintf(os.Stderr, "Scanning: %d files, %d potential duplicates\r", update.Count, update.RawDuplicateCount)
			}
		}
		fmt.Fprintf(os.Stderr, "\n")
	}()

	// set up manual garbage collection routine
	if opts.FreeMemoryInterval > 0 {
		go func() {
			for range time.Tick(time.Duration(opts.FreeMemoryInterval) * time.Second) {
				var m, m2 runtime.MemStats
				if opts.Verbose {
					runtime.ReadMemStats(&m)
				}
				debug.FreeOSMemory()
				if opts.Verbose {
					runtime.ReadMemStats(&m2)
					fmt.Fprintf(
						os.Stderr,
						"\n[%s] Alloc: %s -> %s / Sys: %s -> %s / HeapInuse: %s -> %s / HeapReleased: %s -> %s\n",
						time.Now().Format("15:04:05"),
						humanize.Bytes(m.Alloc),
						humanize.Bytes(m2.Alloc),
						humanize.Bytes(m.Sys),
						humanize.Bytes(m2.Sys),
						humanize.Bytes(m.HeapInuse),
						humanize.Bytes(m2.HeapInuse),
						humanize.Bytes(m.HeapReleased),
						humanize.Bytes(m2.HeapReleased),
					)
				}
			}
		}()
	}

	// wait until scan is complete, then close progress reporting channel
	wg.Wait()
	close(progressChan)
	// TODO figure out why duplicate line of stderr gets printed here
	fmt.Printf("\nFinished scanning Dropbox.\n\n")

	// check for fatal errors
	if dropboxErr != nil {
		panic(dropboxErr)
	}

	// Analyze results for dupe info
	report := analyzeDuplicates(dropboxManifest)
	fmt.Printf("%d duplicate file groups found (%d files, %s).\n\n", len(report.Duplications), report.TotalDuplicateCount, humanize.Bytes(report.TotalDuplicateSize))
	group := 1
	for _, duplication := range report.Duplications {
		fmt.Printf(
			"Group %d (%s, %s)\n",
			group,
			english.Plural(duplication.DuplicateCount, "duplicate file", ""),
			humanize.Bytes(duplication.DuplicateSize),
		)
		for _, f := range duplication.Files {
			fmt.Println(f.Path)
		}
		fmt.Println("")
		group++
	}
	fmt.Println("")
}

func analyzeDuplicates(manifest DropboxManifest) (report *DuplicateReport) {
	// TODO (stretch goal) compute hashes of directories to find wholly duplicated directories (before filtering?)
	report = &DuplicateReport{}
	for hash, files := range manifest {
		if len(files) <= 1 {
			continue
		}
		filteredFiles := filterDuplicateFiles(files)
		if len(filteredFiles) <= 1 {
			continue
		}
		duplicateCount := 0
		duplicateSize := uint64(0)
		for idx, f := range filteredFiles {
			// Don't count first file since we still would presumably keep one
			if idx == 0 {
				continue
			}
			duplicateCount++
			// TODO implement some kind of sanity check to make sure all files in a group have the same size?
			duplicateSize += f.Size
		}

		report.TotalDuplicateCount += duplicateCount
		report.TotalDuplicateSize += duplicateSize
		report.Duplications = append(report.Duplications, &Duplication{
			ContentHash:    hash,
			Files:          filteredFiles,
			DuplicateCount: duplicateCount,
			DuplicateSize:  duplicateSize,
		})
	}
	// sort duplications by size (descending)
	sort.Slice(report.Duplications, func(i, j int) bool {
		return report.Duplications[i].DuplicateSize >= report.Duplications[j].DuplicateSize
	})
	return
}

func filterDuplicateFiles(files []*File) (filteredFiles []*File) {
	for _, file := range files {
		if !ignoreFile(file) {
			filteredFiles = append(filteredFiles, file)
		}
	}
	return
}

func ignoreFile(file *File) bool {
	// TODO extract config
	if file.Size < 1000 {
		return true
	}
	// TODO filter path (like git files or maybe all dotfiles)
	// .....
	return false
}

func getDropboxManifest(progressChan chan<- *scanProgressUpdate, dbxClient *dropbox.Client, rootPath string) (manifest DropboxManifest, err error) {
	manifest = make(DropboxManifest)
	cursor := ""
	keepGoing := true
	retryCount := 0
	fileCount := 0
	dupeCount := 0

	for keepGoing {
		var resp *dropbox.ListFolderOutput
		if cursor != "" {
			arg := &dropbox.ListFolderContinueInput{Cursor: cursor}
			resp, err = dbxClient.Files.ListFolderContinue(arg)
		} else {
			apiPath := rootPath
			if apiPath == "/" {
				apiPath = ""
			}
			arg := &dropbox.ListFolderInput{
				Path:             apiPath,
				Recursive:        true,
				IncludeMediaInfo: false,
				IncludeDeleted:   false,
			}
			resp, err = dbxClient.Files.ListFolder(arg)
		}
		if err != nil {
			// TODO: submit feature request for dropbox client to expose retry_after param
			if strings.HasPrefix(err.Error(), "too_many_requests") {
				fmt.Fprintf(os.Stderr, "\n[%s] [%d retries] Dropbox returned too many requests error, sleeping 60 seconds\n", time.Now().Format("15:04:05"), retryCount)
				// fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				// fmt.Fprintf(os.Stderr, "Response: %v\n", resp)
				retryCount++
				time.Sleep(60 * time.Second)
				continue
			} else if retryCount < 10 { // TODO extract this magic number
				fmt.Fprintf(os.Stderr, "\n[%s] [%d retries] Error: %s - sleeping 1 second and retrying\n", time.Now().Format("15:04:05"), retryCount, err)
				fmt.Fprintf(os.Stderr, "Full Error: %#v\n", err)
				retryCount++
				time.Sleep(1 * time.Second)
				continue
			} else {
				fmt.Fprintf(os.Stderr, "\n[%s] Hit maximum of %d retries; aborting.\n", time.Now().Format("15:04:05"), retryCount)
				return
			}
		}
		// call was successful, reset retryCount
		retryCount = 0
		for _, entry := range resp.Entries {
			if entry.Tag == "file" {

				var relPath string
				relPath, err = normalizePath(rootPath, entry.PathLower)
				if err != nil {
					return
				}

				manifest[entry.ContentHash] = append(manifest[entry.ContentHash], &File{
					Path: relPath,
					Size: entry.Size,
				})

				fileCount++
				if len(manifest[entry.ContentHash]) > 1 {
					dupeCount++
				}
			}
		}

		cursor = resp.Cursor
		keepGoing = resp.HasMore

		progressChan <- &scanProgressUpdate{Count: fileCount, RawDuplicateCount: dupeCount}
	}

	return
}

func normalizePath(root string, entryPath string) (string, error) {
	relPath, err := filepath.Rel(root, entryPath)
	if err != nil {
		return "", err
	}
	if relPath[0:3] == "../" {
		// try lowercase root instead
		relPath, err = filepath.Rel(strings.ToLower(root), entryPath)
		if err != nil {
			return "", err
		}
	}

	// Normalize Unicode combining characters
	relPath = norm.NFC.String(relPath)
	return relPath, nil
}
