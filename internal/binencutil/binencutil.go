package binencutil

import (
	"encoding/binary"
	"fmt"
	"io"
)

func ReadUint64(reader io.Reader) (uint64, error) {
	var buf [8]byte
	_, err := io.ReadFull(reader, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

func WriteUint64(writer io.Writer, value uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := writer.Write(buf[:])
	return err
}

func ReadUint32(reader io.Reader) (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(reader, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func WriteUint32(writer io.Writer, value uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := writer.Write(buf[:])
	return err
}

func ReadUint16(reader io.Reader) (uint16, error) {
	var buf [2]byte
	_, err := io.ReadFull(reader, buf[:])
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf[:]), nil
}

func WriteUint16(writer io.Writer, value uint16) error {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, err := writer.Write(buf[:])
	return err
}

func WriteLongString(writer io.Writer, value string) error {
	if err := WriteUint64(writer, uint64(len(value))); err != nil {
		return err
	}
	_, err := writer.Write([]byte(value))
	return err
}

func ReadLongString(reader io.Reader) (string, error) {
	length, err := ReadUint64(reader)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func WriteShortString(writer io.Writer, value string) error {
	if len(value) > 65535 {
		return fmt.Errorf("string too long: %d", len(value))
	}
	if err := WriteUint16(writer, uint16(len(value))); err != nil {
		return err
	}
	_, err := writer.Write([]byte(value))
	return err
}

func ReadShortString(reader io.Reader) (string, error) {
	length, err := ReadUint16(reader)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func WriteLongBytes(writer io.Writer, value []byte) error {
	if err := WriteUint64(writer, uint64(len(value))); err != nil {
		return err
	}
	_, err := writer.Write(value)
	return err
}

func ReadLongBytes(reader io.Reader) ([]byte, error) {
	length, err := ReadUint64(reader)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func WriteShortBytes(writer io.Writer, value []byte) error {
	if len(value) > 65535 {
		return fmt.Errorf("byte slice too long: %d", len(value))
	}
	if err := WriteUint16(writer, uint16(len(value))); err != nil {
		return err
	}
	_, err := writer.Write(value)
	return err
}

func ReadShortBytes(reader io.Reader) ([]byte, error) {
	length, err := ReadUint16(reader)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func BytesReadUint8(bytes []byte) (uint8, []byte, error) {
	if len(bytes) < 1 {
		return 0, nil, io.EOF
	}
	return bytes[0], bytes[1:], nil
}

func BytesReadUint16(bytes []byte) (uint16, []byte, error) {
	if len(bytes) < 2 {
		return 0, nil, io.EOF
	}
	return binary.LittleEndian.Uint16(bytes), bytes[2:], nil
}

func BytesReadUint32(bytes []byte) (uint32, []byte, error) {
	if len(bytes) < 4 {
		return 0, nil, io.EOF
	}
	return binary.LittleEndian.Uint32(bytes), bytes[4:], nil
}

func BytesReadUint64(bytes []byte) (uint64, []byte, error) {
	if len(bytes) < 8 {
		return 0, nil, io.EOF
	}
	return binary.LittleEndian.Uint64(bytes), bytes[8:], nil
}

func BytesReadInt64(bytes []byte) (int64, []byte, error) {
	if len(bytes) < 8 {
		return 0, nil, io.EOF
	}
	return int64(binary.LittleEndian.Uint64(bytes)), bytes[8:], nil
}
