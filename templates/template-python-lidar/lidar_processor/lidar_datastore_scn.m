function ds = lidar_datastore_scn(files)
%LIDAR_DATASTORE_SCN Returns a datastore object for Wood Group Galion *.scn files
%
% Example file format:
%         Filename:	C:\Lidar\Data\2018\201801\20180101\00\833407_01011800_92.scn
%         Campaign code:	OREC_LDT_SALE_CustomVAD_A1_C
%         Campaign number:	147
%         Rays in scan:	56
%         Start time: 	2018-01-01 00:04:58.224
%         Range gate	Doppler	Intensity	Ray time	Az	El	Pitch	Roll
%         0	-0.568212	0.984639	2018-01-01 00:04:58.224	0.000	56.440	0.196	-0.007
%         1	-0.816645	1.019538	2018-01-01 00:04:58.224	0.000	56.440	0.196	-0.007
%         2	1.017935	1.195653	2018-01-01 00:04:58.224	0.000	56.440	0.196	-0.007
%         3	0.654841	1.188608	2018-01-01 00:04:58.224	0.000	56.440	0.196	-0.007

files(ceil(end/2)).location


% Get and check the number of files
nFiles = numel(files);
if nFiles == 0
    error('At least one file is required to construct a datastore (nFiles <= 0)')
end

% Limit the sequence to the first file_limit files
fileLimit = octue.get('config').file_limit;
if (fileLimit > 0) && (nFiles > fileLimit)
    nFiles = fileLimit;
    files = files(1:fileLimit);
end

% Get a cell array of filenames to form partitions of the datastore
ds_files = cell(nFiles, 1); % preallocate
for iFile = 1:nFiles
    ds_files{iFile} = files(iFile).location;
end


nRangeGates = octue.get('config').n_range_gates;

% Create a partitioned datastore to read from the *.scn files
ds = tabularTextDatastore(ds_files, 'ReadSize', nRangeGates);


end
