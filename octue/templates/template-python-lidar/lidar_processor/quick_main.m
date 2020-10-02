function main()
%MAIN Runs LiDAR analysis for Wood Group Galion LiDAR

% You can get the current analysis from anywhere in your code:
% analysis = octue.get('analysis');

% From which important properties and your configuration can be pulled:
% inputDir  = analysis.InputDir;         % All the input data files are here
% outputDir = analysis.OutputDir;        % Save any results into this directory
% tmpDir    = analysis.TmpDir;           % Save any temporary / cache results here
% logDir    = analysis.LogDir;           % Logs will get saved here
% inputMan  = analysis.InputManifest;    % This manifest object can be used to get names of your input files using the tagging system
% outputMan = analysis.OutputManifest;   % Log results files onto this output manifest to create the results dataset

% Get the config that an analysis is launched with. This gives the analysis
% configuration as a MATLAB structure, validated against the schema.json file
% cfg = analysis.Config;

% Take the *.scn (scan) file sequence and map to a datastore
% scn_files = octue.getFileSequence('extension:scn');
scn_files = [octue.DataFile('/Users/thc29/Source/octue/client-app-sale/data/input/set-sgurr-lidar/G92/OLVAD/2017/201711/20171107/20171107/14')]
scn_ds = lidar_datastore_scn(scn_files);

% Take the *.txt (system temperature) file sequence and map to a datastore
txt_files = octue.getFileSequence('extension:txt');
txt_ds = lidar_datastore_txt(txt_files);

% We need these parameters from the configuration:
% cfg.scan_mode = ['vad_adem'];
% cfg.report_dates = [datetime...];
% cfg.n_range_gates
disp('Preview (scan datastore):')
disp(preview(scn_ds));
disp('Preview (temperature datastore):')
disp(preview(txt_ds));

% Run different analyses
switch lower(cfg.scan_mode(1))
    case 'vad-adem'

        % Run the vad_adem analysis on the entire file sequence
        lidar_vad_adem(scn_files)

        % Mapreduce the timestamps to posix time
%         out_ds = mapreduce(scn_ds, @vad_adem_map, @vad_adem_reduce, mapreducer(0), 'OutputFolder', analysis.OutputDir);

    case 'vad'
        error('MATLAB:NotImplementedYet', 'Analysis for scan_mode selection "%s" not implemented yet.', cfg.scan_mode)

    case 'stare'
        error('MATLAB:NotImplementedYet', 'Analysis scan_mode selection "%s" not implemented yet.', cfg.scan_mode)

    otherwise
        error('MATLAB:NotImplementedYet', 'scan_mode selection "%s" invalid. Try "vad-adem", "vad" or "stare"', cfg.scan_mode)

end

end
