package buffers

func BufferHandleFromFile(path string) RawBufferHandle {
	return &fileBuffer{fpath: path}
}

func CreateCompressedHandle(base RawBufferHandle) CompressedBufferHandle {
	return &compressedBufferHandle{base: base}
}

func InMemoryBufferHandle(data []byte) RawBufferHandle {
	return &inMemoryBuffer{data: data}
}
