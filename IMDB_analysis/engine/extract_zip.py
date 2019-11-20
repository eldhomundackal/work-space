from zipfile import ZipFile

dataset ="C:\work\work-space\IMDB_analysis\Data\ml-1m.zip"

with ZipFile(dataset,'r') as zip:
    print("Extracting files.....")
    zip.printdir()
    zip.extractall(path="C:\work\work-space\IMDB_analysis\Data")

