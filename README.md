# CSVPB

[![PkgGoDev](https://img.shields.io/badge/go.dev-docs-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/alpstable/csvpb)
![Build Status](https://github.com/alpstable/csvpb/actions/workflows/ci.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/alpstable/csvpb)](https://goreportcard.com/report/github.com/alpstable/csvpb)
[![Discord](https://img.shields.io/discord/987810353767403550)](https://discord.gg/3jGYQz74s7)

CSVPB is a library for writing [structpb](https://pkg.go.dev/google.golang.org/protobuf/types/known/structpb#ListValue)-typed data to CSV.

## Installation

```sh
go get github.com/alpstable/csvpb@latest
```

## Usage

The type `structpb` types supported by this package are

- [`ListValue`](https://pkg.go.dev/google.golang.org/protobuf/types/known/structpb#ListValue)

See [here](https://github.com/alpstable/gidari#web-to-storage-examples) for examples.

## Contributing

Follow [this guide](docs/CONTRIBUTING.md) for information on contributing.
