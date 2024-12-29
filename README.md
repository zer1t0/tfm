# TFM

This is the repository with the study tools from my Master's Thesis. The
goal was to train a model capable of distinguish between HTTP error pages and
those with actual user useful content, based on the response content, not the
status code.

The final model is used at [yawb](https://github.com/zer1t0/yawb).

## Setup

First, you need to install the required Python packages:
```
pip3 install -r requirements.txt
```

Then, in case you want to use the trained distilbert model, you need to install
PyTorch. The following command will install it to run in CPU:
```
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
```

In order to execute the script `gen-dmoz-test-dataset.sh`, you may also need the
[httpx](https://github.com/projectdiscovery/httpx/releases) tool. Notice that
this is not the one installed with the Python package with the same name.

Additionally you may want to install jupyter to run the notebooks:
```
pip3 install jupyter
```

WARNING: Installing jupyter install the httpx Python package and it may
conflict with httpx tool name, so you may want to change the httpx tool name to
something like httpx-pd.

## Use

You can use the models included into the `models` folder. In case you want to
generate them, you can use the `ml-model-comparison.ipynb` and
`distilbert-model-training.ipynb`.

You can use the datasets included in `datasets` folder. In case you want to
generate those by yourself, you can use the `gen-cc-dataset.sh` and
`gen-dmoz-test-dataset.sh` scripts, but aware that the later includes a
interactive step to label the samples.

## Author

Eloy Pérez González
