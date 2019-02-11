import pandas as pd
from IPython.display import display
from IPython.core.display import HTML
import os.path


def test_dispay_pandas():
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "../../python_sample.csv")
    df = pd.read_csv(path)
    pd.options.display.max_columns = None
    display(HTML(df.to_html()))


if __name__ == '__main__':
    test_dispay_pandas()