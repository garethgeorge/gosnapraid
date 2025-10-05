package snapshot

const (
	defaultReadBufferSize = 128 * 1024 * 1024
)

// // SnapshotGenerator helps produce a snapshot of the filesystem.
// type SnapshotGenerator struct {
// 	RootDirectory    string
// 	HashingAlgorithm hashing.ContentHashAlgorithm
// }

// func NewSnapshotGenerator(rootDirectory string) *SnapshotGenerator {
// 	g := &SnapshotGenerator{
// 		RootDirectory:    rootDirectory,
// 		HashingAlgorithm: hashing.CONTENT_HASH_ALGORITHM_XXH3,
// 	}
// 	return g
// }

// func (g *SnapshotGenerator) UpdateNode(dirPath string, node *SnapshotNode, prevNodeSnapshot *SnapshotNode) (*Snapshot, *errors.ErrorAggregation) {
// 	errorAgg := errors.NewErrorAggregation(0)

// 	// Read the files in dirPath
// 	files, err := os.ReadDir(dirPath)
// 	if err != nil {
// 		errorAgg.Add(err)
// 		return nil, errorAgg
// 	}

// 	type metadataAndName struct {
// 		DiffingMetadata
// 		Name string
// 	}

// 	// Vector of new entries
// 	metadata := make([]metadataAndName, 0, len(files))
// 	for _, file := range files {
// 		md, err := g.diffingMetadataForEntry(file)
// 		if err != nil {
// 			errorAgg.Add(errors.NewFixedCause("get directory metadata", fmt.Errorf("for file %s/%s: %w", dirPath, file.Name(), err)))
// 			continue
// 		}
// 		metadata = append(metadata, metadataAndName{
// 			DiffingMetadata: md,
// 			Name:            file.Name(),
// 		})
// 	}

// 	// Vector of existing entries
// 	prevMetadata := make([]metadataAndName, 0, len(prevNodeSnapshot.Children))
// 	for _, child := range prevNodeSnapshot.Children {
// 		prevMetadata = append(prevMetadata, metadataAndName{
// 			DiffingMetadata: child.Metadata.ToDiffingMetadata(),
// 			Name:            child.Metadata.Name,
// 		})
// 	}

// 	// Sort each list by metadata
// 	sort.Slice(metadata, func(i, j int) bool {
// 		return metadata[i].CompareByIno(&metadata[j].DiffingMetadata) < 0
// 	})
// 	sort.Slice(prevMetadata, func(i, j int) bool {
// 		return prevMetadata[i].CompareByIno(&prevMetadata[j].DiffingMetadata) < 0
// 	})

// 	// Use the old metadata + the new metadata to ensure that we have a hash for every file
// 	// Heuristic: if the metadata e.g. modtime, etc is unchanged, assume the hash is unchanged.
// 	i := 0
// 	j := 0

// 	newMetadata := make([]metadataAndName, 0, len(metadata))
// 	updatedMetadata := make([]metadataAndName, 0, len(metadata))

// 	for i < len(metadata) && j < len(prevMetadata) {
// 		cmp := metadata[i].CompareByIno(&prevMetadata[j].DiffingMetadata)
// 		if cmp < 0 {
// 			i++
// 			newMetadata = append(newMetadata, metadata[i])
// 		} else if cmp > 0 {
// 			j++
// 		} else {
// 			if metadata[i].CompareIgnoreHash(&prevMetadata[j].DiffingMetadata) == 0 {
// 				// Assume hash is most likely unchanged
// 				metadata[i].Hash = prevMetadata[j].Hash
// 			} else {
// 				// Read file and compute the hash
// 				hash, err := g.computeHashForFile(dirPath + string(os.PathSeparator) + metadata[i].Name)
// 				if err != nil {
// 					errorAgg.Add(errors.NewFixedCause("compute hash", fmt.Errorf("for file %s/%s: %w", dirPath, metadata[i].Name, err)))
// 					continue
// 				}
// 				metadata[i].Hash = hash
// 				updatedMetadata = append(updatedMetadata, metadata[i])
// 			}
// 			i++
// 			j++
// 		}
// 	}

// 	return nil, errorAgg
// }

// func (g *SnapshotGenerator) diffingMetadataForEntry(file os.DirEntry) (DiffingMetadata, error) {
// 	info, err := file.Info()
// 	if err != nil {
// 		return DiffingMetadata{}, err
// 	}
// 	ino, gen, err := getInoAndGen(info)
// 	if err != nil {
// 		return DiffingMetadata{}, err
// 	}

// 	return DiffingMetadata{
// 		Ino:     ino,
// 		Gen:     gen,
// 		Size:    uint64(info.Size()),
// 		Mode:    info.Mode(),
// 		ModTime: info.ModTime().Unix(),
// 	}, nil
// }

// func (g *SnapshotGenerator) computeHashForFile(fpath string) (hashing.ContentHash, error) {
// 	// Open the file in read mode
// 	file, err := os.Open(fpath)
// 	if err != nil {
// 		return hashing.ContentHash{}, err
// 	}
// 	defer file.Close()

// 	// Create a buffered reader and use it to issue IO when computing the hash
// 	reader := bufio.NewReaderSize(file, defaultReadBufferSize)
// 	hash, err := hashing.ContentHashFromReader(g.HashingAlgorithm, reader)
// 	if err != nil {
// 		return hashing.ContentHash{}, err
// 	}
// 	return hash, nil
// }
