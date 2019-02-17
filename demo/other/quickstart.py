import pandas as pd
from IPython.display import display
from IPython.core.display import HTML
import os.path
import matplotlib.pyplot as plt
from pylab import *


def test_dispay_pandas():
    my_path = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(my_path, "../../python_sample.csv")
    df = pd.read_csv(path)
    pd.options.display.max_columns = None

    #show in jupyter notebook
    # display(HTML(df.to_html()))
    df.plot()
    plt.show()

def test_matplotlib():

    n = 12
    X = np.arange(n)
    Y1 = (1 - X / float(n)) * np.random.uniform(0.5, 1.0, n)
    Y2 = (1 - X / float(n)) * np.random.uniform(0.5, 1.0, n)

    bar(X, +Y1, facecolor='#9999ff', edgecolor='white')
    bar(X, -Y2, facecolor='#ff9999', edgecolor='white')

    for x, y in zip(X, Y1):
        text(x + 0.4, y + 0.05, '%.2f' % y, ha='center', va='bottom')

    ylim(-1.25, +1.25)
    show()

if __name__ == '__main__':
    test_dispay_pandas()
    # test_matplotlib()