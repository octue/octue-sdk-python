# More information on downloading datafiles

- To avoid unnecessary data transfer and costs, cloud datafiles are not
  downloaded locally [until necessary](datafiles.md/#automatic-lazy-downloading)
- When downloaded, they are downloaded by default to a temporary local
  file that will exist at least as long as the python session is running
- Calling `Datafile.download` or using `Datafile.local_path` again will
  not re-download the file
- Any changes made to the datafile via the `Datafile.open` method are
  made to the local copy and then synced with the cloud object

!!! warning

    External changes to cloud files will not be synced locally unless the
    datafile is re-instantiated.

- If you want a cloud datafile to be downloaded to a permanent location,
  you can do one of:

  ```python
  datafile.download(local_path="my/local/path.csv")

  datafile.local_path = "my/local/path.csv"
  ```

- To pre-set a permanent download location on instantiation, run:

  ```python
  datafile = Datafile(
      "gs://my-bucket/path/to/file.dat",
      local_path="my/local/path.csv",
  )
  ```
