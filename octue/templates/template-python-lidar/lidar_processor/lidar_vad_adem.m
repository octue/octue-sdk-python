function lidar_vad_adem(files)
%LIDAR_VAD_ADEM Runs VAD + ADEM analyses on a datastore of beam-coordinate

% Get the octue configuration and logger
logger = octue.get('logger');
config = octue.get('config');
nRangeGates = config.n_range_gates;
beamAngleTolerance = config.beam_angle_tolerance;
intensityThreshold = config.intensity_threshold;
rangeGateOverlap = config.range_gate_overlap;
rangeGateOffset = 12; % TODO


% TODO we can calculate the number of range gates from the file contents


% Get and check the number of files
nFiles = numel(files);
if nFiles == 0
    error('At least one file is required to construct a datastore (nFiles <= 0)')
end

% Limit the sequence to the first file_limit files
fileLimit = config.file_limit;
if (fileLimit > 0) && (nFiles > fileLimit)
    logger.warn(fprintf('Limiting max. possible number of files (%i) to %i', nFiles, fileLimit))
    nFiles = fileLimit;
end

% Iterate through all the .scn files
for i = 1:nFiles

    logger.info(fprintf('Reading scn file %i of %i', i, nFiles))

    % Get the .scn file as a datastore
    file = files(i);
    ds = lidar_datastore_scn(file);

    % Read the whole file at once, as the scan files aren't large and it'll be quicker
    scan = ds.readall;

    % Checks on the data
    nRows = size(scan, 1);
    nBeams = nRows/nRangeGates;
    if (nRows/4) ~= round(nRows/4)
        error('LIDAR:IncompleteScan', 'Number of rows in the scan must be divisible by 4 (incomplete scan)')
    elseif (nBeams) ~= round(nBeams)
        error('LIDAR:IncorrectNRangeGates', 'Number of rows in the scan must be divisible by the number of range gates (incomplete scan or incorrect no. range gates)')
    end

    % Preallocate intermediate arrays
    doppler = zeros(nRangeGates, nBeams); % Transposed for accelerated write
    intensity = zeros(nRangeGates, nBeams); % Transposed for accelerated write
    posixRayTime = zeros(nBeams, 1);
    az = zeros(nBeams, 1);
    el = zeros(nBeams, 1);
    pitch = zeros(nBeams, 1);
    roll = zeros(nBeams, 1);

    % Read each beam at a time
    for iBeam = 1:nBeams

        % Index into the beam
        beamInds = (1:nRangeGates) + (iBeam-1)*nRangeGates;
        beam = scan(beamInds, :);

        % Change the datestamp to a posix time
        % TODO this is psychotic. There's GOT to be a better way which is just
        % as generic as using datenum's datetime string parser.
        posixRayTime(iBeam) = posixtime(datetime(datevec(datenum(beam.RayTime{1}))));

        % Extract other variables which are fixed for the beam
        az(iBeam) = beam.Az(1);
        el(iBeam) = beam.El(1);
        pitch(iBeam) = beam.Pitch(1);
        roll(iBeam) = beam.Roll(1);

        % Stream data into a matrix, columnar orientation
        doppler(:, iBeam) = beam.Doppler;
        intensity(:, iBeam) = beam.Intensity;

    end

    % Analyse the beam groups (repeated firings of the same beam)
    nBeamsPerGroup = nBeams/4;

    % Transpose the doppler and intensity arrays for quickest read access. So we
    % have intermediate [nBeams x nRangeGates] arrays, then reshape to have
    % [nBeamsPerGroup x nGroups x nRangeGates] arrays, where nGroups = 4 always
    doppler = reshape(doppler', [nBeamsPerGroup, 4, nRangeGates]);
    intensity = reshape(intensity', [nBeamsPerGroup, 4, nRangeGates]);

    % Preallocate the group-reduced az, el, pitch, roll
    azGroup = zeros(1, 4);
    elGroup = zeros(1, 4);
    pitchGroup = zeros(1, 4);
    rollGroup = zeros(1, 4);

    % Time difference between beams (has to be done within a group to ignore the
    % time in changing orientation between groups)
    dtGroup = zeros(1,4);

    for iGroup = 1:4

        % Index into the beamgroup
        groupInds = (1:nBeamsPerGroup) + (iGroup-1)*nBeamsPerGroup;

        % Check that they're all at the same az, el, within tolerance
        if max(az(groupInds)) - min(az(groupInds)) > beamAngleTolerance
            error('LIDAR:BeamAngleTolerance', 'Beam angle tolerance within a group is exceeded in the azimuth.')
        end
        if max(el(groupInds)) - min(el(groupInds)) > beamAngleTolerance
            error('LIDAR:BeamAngleTolerance', 'Beam angle tolerance within a group is exceeded in the azimuth.')
        end

        % TODO checks on pitch, roll consistency

        % TODO Check that the beamgroup timeseries is consistently stepped to within
        % a percentage tolerance

        % There could be the occasional skipped beam or write to disk, we want
        % to ignore this for robustness
        dtGroup(iGroup) = median(diff(posixRayTime(groupInds)));

        % Use the averages from the beamgroup to collapse. Median less sensitive
        % to one or two outliers than mean, although outliers are below the
        % tolerance anyway so not a huge issue.
        azGroup(iGroup) = median(az(groupInds));
        elGroup(iGroup) = median(el(groupInds));
        pitchGroup(iGroup) = median(pitch(groupInds));
        rollGroup(iGroup) = median(roll(groupInds));

    end

    % Compute the heights for the range gates
    elevation = median(elGroup);
    rangeGateDistance = (0:nRangeGates-1)'*rangeGateOverlap + rangeGateOffset;
    rangeGateHeight = sind(elevation)*rangeGateDistance;

    % Compute the cartesian velocity for the groups, excluding data thats too
    % noisy based on the given threshold
    [u] = vad(azGroup, elGroup, doppler, intensity, intensityThreshold, false);

    % TODO check on consistency of DT between beamgroups (it should be very
    % close to the same)

    % Collapse the DT for the beam groups, find the sampling frequency
    dt = median(dtGroup);
    fs = 1/dt;

    % Find the frequency vector (column orientation)
    f = fs*(0:(nBeamsPerGroup/2))'/nBeamsPerGroup;

    % Take the two-sided power spectrum of the doppler velocity (beam coords)
    p2 = abs(fft(doppler, nBeamsPerGroup, 1)/nBeamsPerGroup);

    % Compute the two-sided spectrum P2, then the single-sided Power
    % spectrum P1 based on P2 and the even-valued signal length nBeamsPerGroup
    p1 = p2(1:(floor(nBeamsPerGroup/2+1)), :, :);
    p1(2:end-1,:,:) = 2*p1(2:end-1,:,:);

    % TODO consider and review the denoising options
    p1(2:end, :, :) = smoothdata(smoothdata(p1(2:end, :, :), 1), 2);

    % Compute the power spectral density
    psd = bsxfun(@ldivide, p1, f);

    % Use the VAD technique to align spectra
    psdCart = vadSpectrum(azGroup, elGroup, psd);

    % Compute the horizontal velocity component
    uh = sqrt(sum(u(:, 1:2).^2, 2));

    % Fit the boundary layer model
    [profile, fit, sProfile] = lidar_adem(rangeGateHeight, u, psdCart, f); %#ok<ASGLU>

    % Plot the wind vector profile (2D)
    data = {struct('x', u(:, 1), 'y', rangeGateHeight(:), 'type', 'scatter', 'name', 'ux (m/s)'), ...
            struct('x', u(:, 2), 'y', rangeGateHeight(:), 'type', 'scatter', 'name', 'uy (m/s)'), ...
            struct('x', u(:, 3), 'y', rangeGateHeight(:), 'type', 'scatter', 'name', 'uz (m/s)'), ...
            struct('x', uh, 'y', rangeGateHeight(:), 'type', 'scatter', 'name', 'uh (m/s)'),...
            struct('x', fit.uh, 'y', rangeGateHeight(:), 'type', 'scatter', 'name', 'uh (Fitted) (m/s)')};
    layout = struct('width', 1600, 'height', 1200);
    layout.title = 'Wind speed component profiles';
    layout.xaxis = struct('title', 'u (m/s)', 'autorange', true);
    layout.yaxis = struct('title', 'Height z from ground (m)', 'autorange', true);
    tags = 'contents:velocityprofile type:figure';
    octue.addFigure(data, layout, tags);

    % Plot the wind vector profile (3D)
    data = struct('x', u(:,1), ...
                  'y', u(:,2), ...
                  'z', rangeGateHeight(:), ...
                  'type', 'scatter3d');
    layout = struct('width', 1600, 'height', 1200);
    layout.title = 'Horizontal wind speed profile';
    layout.scene = struct();
    layout.scene.xaxis = struct('title', 'u_x (m/s)', 'autorange', true);
    layout.scene.yaxis = struct('title', 'u_y (m/s)', 'autorange', true);
    layout.scene.zaxis = struct('title', 'Height z from ground (m)', 'autorange', true);
    tags = 'contents:velocityprofile:3d type:figure';
    octue.addFigure(data, layout, tags);

    % Plot spectral densities for the beamgroup
    [x, y] = meshgrid(f, rangeGateHeight);
    for iGroup = 1:4
        data = struct('x', x, ...
                      'y', y, ...
                      'z', squeeze(psd(:,iGroup,:))', ...
                      'colorscale', 'YlGnBu', ...
                      'type', 'surface');
        layout = struct('width', 1600, 'height', 1200);
        layout.title = ['Single-Sided Amplitude Power Spectral Density of Beam Group ' num2str(iGroup)];
        layout.scene = struct();
        layout.scene.xaxis = struct('title', 'f (Hz)', 'autorange', true);
        layout.scene.yaxis = struct('title', 'Height from ground (m)', 'autorange', true);
        layout.scene.zaxis = struct('title', ['|S' num2str(iGroup) '(f)|/f'], 'autorange', true);
        tags = 'contents:powerspectrum type:figure:surface';
        octue.addFigure(data, layout, tags);
    end

    % Plot spectral densities in the cartesian frame
    for iDirn = 1:3
        data = struct('x', x, ...
                      'y', y, ...
                      'z', squeeze(psdCart(:,iDirn,:))', ...
                      'colorscale', 'YlGnBu', ...
                      'type', 'surface');
        layout = struct('width', 1600, 'height', 1200);
        layout.title = ['Single-Sided Amplitude Power Spectral Density |S' num2str(iDirn) '(f)|/f'];
        layout.scene = struct();
        layout.scene.xaxis = struct('title', 'f (Hz)', 'autorange', true);
        layout.scene.yaxis = struct('title', 'Height from ground (m)', 'autorange', true);
        layout.scene.zaxis = struct('title', ['|S' num2str(iDirn) '(f)|/f'], 'autorange', true);
        tags = 'contents:powerspectrum type:figure:surface';
        octue.addFigure(data, layout, tags);
    end

    % change densities to normalise them
    psdPremultipliedNormalised = psdCart/sProfile.Utau.^2;
    for iDirn = 1
        data = struct('x', x, ...
                      'y', y, ...
                      'z', squeeze(psdPremultipliedNormalised(:,iDirn,:))', ...
                      'colorscale', 'YlGnBu', ...
                      'type', 'surface');
        layout = struct('width', 1600, 'height', 1200);
        layout.title = 'Single-Sided Amplitude Power Spectral Density Psi/k1zUtau^2';
        layout.scene = struct();
        layout.scene.xaxis = struct('title', 'f (Hz)', 'autorange', true);
        layout.scene.yaxis = struct('title', 'Height from ground (m)', 'autorange', true);
        layout.scene.zaxis = struct('title', ['|S' num2str(iDirn) '(f)|/f'], 'autorange', true);
        tags = 'contents:powerspectrum type:figure:surface';
        octue.addFigure(data, layout, tags);
    end

    % Plot the Reynolds Stress vector profiles (2D)
    data = {struct('x', sProfile.R(:, 1), 'y', sProfile.z(:), 'type', 'scatter', 'name', 'R11'), ...
            struct('x', sProfile.R(:, 2), 'y', sProfile.z(:), 'type', 'scatter', 'name', 'R12'), ...
            struct('x', sProfile.R(:, 3), 'y', sProfile.z(:), 'type', 'scatter', 'name', 'R13'), ...
            struct('x', sProfile.R(:, 4), 'y', sProfile.z(:), 'type', 'scatter', 'name', 'R22'), ...
            struct('x', sProfile.R(:, 5), 'y', sProfile.z(:), 'type', 'scatter', 'name', 'R23'), ...
            struct('x', sProfile.R(:, 6), 'y', sProfile.z(:), 'type', 'scatter', 'name', 'R33')};
    layout = struct('width', 1600, 'height', 1200);
    layout.title = 'Reynolds Stress profiles';
    layout.xaxis = struct('title', 'Reynolds Stress', 'autorange', true);
    layout.yaxis = struct('title', 'Height z from ground (m)', 'autorange', true);
    tags = 'contents:velocityprofile type:figure';
    octue.addFigure(data, layout, tags);

    %     % Plot psi shapes
    %     disp('Plotting')
    %     size(sProfile.z)
    %     size(sProfile.k1z)
    %     size(sProfile.Psi)
    %     k1z = sProfile.k1z(:,1:100:end);
    %     psi = sProfile.Psi(1:300:end,1:30:end,1);
    %     psi(isnan(psi)) = 0;
    %     [x, y] = meshgrid(1:size(psi,1), 1:size(psi,2));
    %     for iDirn = 1
    %         data = struct('x', x, ...
    %                       'y', y, ...
    %                       'z', psi', ...
    %                       'colorscale', 'YlGnBu', ...
    %                       'type', 'surface');
    %         layout = struct('width', 1600, 'height', 1200);
    %         layout.title = ['psi'];
    %         layout.scene = struct();
    %         layout.scene.xaxis = struct('title', 'k1z', 'autorange', true);
    %         layout.scene.yaxis = struct('title', 'Height from ground (m)', 'autorange', true);
    %         layout.scene.zaxis = struct('title', ['Psi' num2str(iDirn) ], 'autorange', true);
    %
    %         tags = 'contents:power type:figure:surface';
    %         octue.addFigure(data, layout, tags);
    %     end
end
