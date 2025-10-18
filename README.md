# gosnapraid2

## Building

This project uses [Protocol Buffers](https://developers.google.com/protocol-buffers) and [Buf](https://buf.build) to generate Go code from the `.proto` files.

### Prerequisites

1.  **Install Buf:**

    Follow the instructions on the [Buf website](https://docs.buf.build/installation) to install `buf`.

2.  **Install Protobuf Go Plugins:**

    Install the Go plugins for protobuf and vtprotobuf:

    ```bash
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest

    ```

### Generating Code

Once the prerequisites are installed, you can generate the Go code by running the following command in the root of the project:

```bash
buf generate
```
