import sys
import pandas as pd
import pyarrow as pa

def coerceInt(series):
    return series.astype('int32')

def coerceString(series):
    return series.astype('unicode').astype('category')

def batchFromCsv(fd):
    df = pd.read_csv(fd)

    return pa.Table.from_pandas(pd.DataFrame(
        {
            "gameid": coerceInt(df.gameid),
            "tourneyid": coerceInt(df.tourneyid),
            "tie": df.tie,
            "winnername": coerceString(df.winnername),
            "losername": coerceString(df.losername),
            "winnerscore": coerceInt(df.winnerscore),
            "loserscore": coerceInt(df.loserscore),
            "year": pd.to_datetime(df.date).dt.year.astype('uint16')
        }
    ))
    # return pa.Table.from_pandas(df)
    # table = pa.Table.from_pandas(df)
    

    # return pa.Table.from_pandas(df)

def writeArrowStream(fd, batch):
    with open(fd, 'bw') as f:
        writer = pa.RecordBatchFileWriter(f, batch.schema)
        writer.write(batch)
        writer.close()

if __name__ == "__main__":
    from urllib.request import urlopen
    print('running......')
    # ifd = sys.stdin  if sys.argv[1] == '-' else urlopen(sys.argv[1])
    ifd = sys.stdin  if sys.argv[1] == '-' else sys.argv[1]
    ofd = sys.stdout if sys.argv[2] == '-' else sys.argv[2]

    writeArrowStream(ofd, batchFromCsv(ifd))