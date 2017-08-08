# PyAzBlob
Python tool to upload files into Azure Storage Blob Service from local file system.

## Disclaimer
[AzCopy](https://docs.microsoft.com/en-us/azure/storage/storage-use-azcopy) is the official tool from Microsoft that, among many other things, offers bulk upload of files from local file system to Azure Storage Blob Service. PyAzBlob is a simple console application created in a few hours, mostly for fun and to practice with [Microsoft Azure Storage SDK for Python](https://github.com/Azure/azure-storage-python). However, it does implement a couple of features that I find useful for my personal use, that are not available in AzCopy.

```
  _____                     ____  _       _                
 |  __ \         /\        |  _ \| |     | |               
 | |__) |   _   /  \    ___| |_) | | ___ | |__             
 |  ___/ | | | / /\ \  |_  /  _ <| |/ _ \| '_ \            
 | |   | |_| |/ ____ \  / /| |_) | | (_) | |_) |           
 |_|    \__, /_/    \_\/___|____/|_|\___/|_.__/            
         __/ |                                             
        |___/                                              
```

## Features
* user friendly console application with integrated help
* recursive upload of files, keeping the same folder structure of local file system
* definition of ignored files by Unix-style glob patterns
* logs uploaded files one by one, to skip re-uploading same files to same Azure Storage container in following runs
* supports definition of Azure Storage keys inside environmental variables or in .ini file
* two implementations: event-based (asynchronous) implementation and synchronous implementation, described below under _Branches_

## Branches
This repository has two branches, with two implementations of the application:
* [**async** branch](https://github.com/RobertoPrevato/PyAzBlob/tree/async), with event-based, asynchronous version that only supports files smaller than 64MB, using aiohttp framework
* [**master** branch](https://github.com/RobertoPrevato/PyAzBlob) with synchronous version, using the official [Microsoft Azure Storage SDK for Python](https://github.com/Azure/azure-storage-python), which automatically handles chunked upload of files greater than 64MB

The asynchronous version offers best performance, especially for small files in big number. The two implementations have smaller differences: the async version requires a [_Shared Access Signature (SAS)_](https://docs.microsoft.com/en-us/azure/storage/storage-dotnet-shared-access-signature-part-1) from a storage account, whereas the sync version requires _storage account name and an administrative key_.

## This branch
This branch (**async**) contains the event based implementation, using Python 3.4 > built-in Asynchronous I/O framework, called [asyncio](https://docs.python.org/3/library/asyncio.html), and its HTTP client/server implementation, called [aiohttp](http://aiohttp.readthedocs.io/en/stable/), to implement an event based bulk uploader using Azure Blob Storage REST api.

## Requirements
* Python 3.4 =>
* Azure Storage

## How to use
1. Download or clone this repository
```bash
# clone repository (async branch):
git clone https://github.com/RobertoPrevato/PyAzBlob.git -b async
```

### 2. Create Python virtual environment and restore dependencies

```bash
# Linux:
python3 -m venv env

env/bin/pip install -r requirements.txt
```

```bash
# Windows:
py -3 -m venv env
env\Scripts\pip install -r requirements.txt
```

### 3. Activate Python virtual environment (optional)

```bash
# Linux:
source env/bin/activate
```

```bash
# Windows:
env\Scripts\activate.bat
```

### 4. Configure the Azure Storage

Configure a _Shared Access Signature_ in file `settings.ini` or, alternatively, in an environmental variable called _PYAZ_ACCOUNT_SAS_, which is read by Python console application when running the script. 

**Recommendations**: if you are creating an Azure Storage for backups, use _Standard_ performance and [_LRS_ (Locally Redundant Storage)](https://docs.microsoft.com/en-us/azure/storage/storage-redundancy#locally-redundant-storage). Make sure to use *Private* containers if you want your data to be kept private.

#### 4.1 Useful links
* [Create a Storage Account](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account)
* [Blob Storage Account pricing](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/)

Storage account name and settings can be found in the Azure Portal under `Settings > Access keys`.

![Azure Storage Settings](https://gist.githubusercontent.com/RobertoPrevato/9ff1fc2fe8acf15bbbe6094a836697f8/raw/0d100871f2233e0ea415ab21a8546330a8703534/azure-storage-sas.png)

### 5. Run the console application

If the environment was activated, use "python"; otherwise: `env\bin\python` in Linux or `env/Scripts/python` in Windows.

```bash
# display the help:
python pyazblob.py -h
```

![Help](https://gist.githubusercontent.com/RobertoPrevato/9ff1fc2fe8acf15bbbe6094a836697f8/raw/01175cde3c8f69c2a8496a1ae0ad2c1d4fbcc6a4/pyazblob-help-async.png)

Example: upload all files from `/home/username/Pictures/` recursively, and keeping folder structure starting from `/Pictures/`:

```bash
python pyazblob.py -p /home/username/Pictures/ -c /home/username/ -r
```

Upload all files from `C:\Users\username\Documents\` recursively, keeping folder structure starting from `\Documents\`:
```bash
python pyazblob.py -p C:\Users\username\Documents\ -c C:\Users\username\
```

![Bulk upload](https://gist.githubusercontent.com/RobertoPrevato/9ff1fc2fe8acf15bbbe6094a836697f8/raw/0558d5bbf903e1991f69befb39e9e078f446c50e/pyaz-upload.jpg)

## Configuration options
* define ignored file paths (Unix-style globs) using `.pyazblobignore` file
* define Azure Storage key, name and destination container name using `settings.ini` file, or following environmental variables

| Name                | Scope                              |
|---------------------|------------------------------------|
| PYAZ_ACCOUNT_NAME   | account name                       |
| PYAZ_ACCOUNT_SAS    | account shared access signature    |
| PYAZ_CONTAINER_NAME | container name                     |
