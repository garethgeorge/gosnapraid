package buffers

func BufferHandleFromFile(path string) BufferHandle {
	return &fileBuffer{fpath: path}
}

func CompressedBufferHandle(base BufferHandle) BufferHandle {
	return &compressedBufferHandle{base: base}
}

func InMemoryBufferHandle(data []byte) BufferHandle {
	return &inMemoryBuffer{data: data}
}
