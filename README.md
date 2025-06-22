<p align="center">
    <img height="60px" width="60px" src="media/logo.png"/>
    <h1 align="center">collie</h1>    
</p>

<p align="center">
    <a href="https://github.com/noppaz/collie/actions"><img src="https://badgen.net/github/checks/noppaz/collie" /></a>
    <a href="LICENSE.txt"><img src="https://badgen.net/github/license/noppaz/collie" /></a>
    <a href="https://github.com/noppaz/collie/releases"><img src="https://badgen.net/github/release/noppaz/collie/stable" /></a>
</p>

<p align="center">A Parquet file CLI explorer with TUI elements.</p>

## Installation

Download binaries from the [release page](https://github.com/noppaz/collie/releases).

Install from source with

```
go install
```

## Caveats

Column compressions LZO and LZ4 are not supported by collie due to arrow-go/parquet package [not supporting them](https://github.com/apache/arrow-go/blob/main/parquet/compress/compress.go#L55-L59).
