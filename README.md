# mriqc-export

Simple utility script for downloading files from the [MRIQC Web API](https://mriqc.nimh.nih.gov/). The results are stored in a parquet table.

Dependencies are managed with [`pixi`](http://pixi.sh/). As an example, the first ~50000 T1w records can be downloaded with the following command.

```shell
# each page contains 50 records
pixi run python download.py --max-pages 1000 T1w
```
