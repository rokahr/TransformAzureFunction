# TransformAzureFunction
This repository hosts a template for an Azure Function that reads data from adls2 into a pandas DF. There is a code section where you can implement your python magic. The end writes the DF back to adls.

It has to be invoked over a Post request similar with the following JSON body:
```json
{
  "in_container": "raw",
  "in_path": "Sales",
  "in_file": "Sales.csv",
  "out_container": "transformed",
  "out_path": "Sales",
  "out_file": "Sales.parquet"
}
```
This works if it's deployed to an Azure Function where a system assigned MI is configured. Make sure that the MI has the correct permissions on the storage account
As an alternative, the function can be called locally where the account key is passed as a parameter for testing:
```json
{
  "in_container": "raw",
  "in_path": "Sales",
  "in_file": "Sales.csv",
  "out_container": "transformed",
  "out_path": "Sales",
  "out_file": "Sales.parquet",
  "storagekey": "YourStorageKey"
}
```
