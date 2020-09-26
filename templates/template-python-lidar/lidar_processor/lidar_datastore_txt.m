function ds = lidar_datastore_txt(files)
%LIDAR_DATASTORE_TXT Returns a datastore object for Wood Group Galion *.txt system temperature files
%
% Example file format:
%         date/time	int temp	int hum	o/s temp	o/s hum	o/s press	temp control	input voltage
%
%         2018-01-01 00:14:51.434	16.75	14.18	4.20	100.00	982.20	0	25.74
%         2018-01-01 00:24:50.184	16.75	14.18	4.30	100.00	982.20	0	25.78
%         2018-01-01 00:34:49.206	16.75	14.18	4.30	100.00	982.20	0	25.74
%         2018-01-01 00:44:48.321	16.75	14.18	4.50	100.00	982.20	0	25.69
%         2018-01-01 00:54:47.497	16.75	14.18	4.60	100.00	982.10	0	25.74
%         2018-01-01 01:04:54.564	16.75	14.18	4.60	100.00	982.20	0	25.69
%

% Get and check the number of files
nFiles = numel(files);
if nFiles == 0
    error('At least one file is required to construct a datastore (nFiles <= 0)')
end

% Get a cell array of filenames to form partitions of the datastore
ds_files = cell(nFiles, 1); % preallocate
for iFile = 1:nFiles
    ds_files{iFile} = files(iFile).location;
end

% Create a partitioned datastore to read from the .txt files
ds = tabularTextDatastore(ds_files, ...
               'NumHeaderLines', 2, ...
               'ReadVariableNames', false, ...
               'VariableNames', {'DateTime', 'InternalTemperature', 'InternalHumidity', 'ExternalTemperature', 'ExternalHumidity', 'ExternalPressure','TemperatureControl', 'InputVoltage'}, ...
               'Delimiter', '\t');

end
