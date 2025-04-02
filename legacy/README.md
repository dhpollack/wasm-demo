# Legacy Python Code

This code was part of the original project.  It has been kept for educational purposes and not all parts of it work.  Notably, the producer will produce csv lines, whereas the rust version produces json.  Thus the transform will fail because it is expecting json and not csv.  Also the rust version has two additional categorical features (order_type and vehicle_type), which are not present in this version.  I will not be fixing this, but will leave it as an exercise for the user to do so if you want to try to use the python version of this program.

## Usage

The only part of this that has been tested is the data exploration notebook `stats.ipynb`.  Here is how to run the notebook server using `uv`:

```bash
# locally
uv run pip install .
uv run jupyterlab
# docker image
docker build -t legacy . && docker run legacy
```
