function u = vad(az, el, doppler, intensity, threshold, fillMissing)
%VAD Computes VAD from multiple beams using Velocity Azimuth Display method
%
% Syntax:
%
%   u = VAD(az, el, doppler, intensity) Computes velocity from a series of beams
%   at some azimuth and elevation.
%
% Inputs:
%
%       doppler         [nBeamsPerGroup x nGroups x nRangeGates]
%
%                       For most conventional VAD, nBeamsPerGroup=1, but
%                       accepting this ordering allows us to do repeated groups
%                       for higher frequency analysis, like in the SALE project.
%
%       az              [1 x nGroups] or [nGroups x 1]
%
%                       Contains the azimuth angle (usually denoted theta) of
%                       the beamgroup measured clockwise from true north [1], in
%                       degrees.
%
%       el              [1 x nGroups] or [nGroups x 1]
%
%                       Contains the elevation angle of the beamgroup from the
%                       ground (usually denoted phi), in degrees.
%
%       fillMissing     [1 x 1] bool
%
%                       If true, missing (NaN) elements in the output array are
%                       filled using a 5 point median window.
%
% Outputs:
%
%       u               [nRangeGates x 3]
%                       Matrix of [ux, uy, uz] velocity components in m/s, where
%                       ux is positive in the true East direction, uy is
%                       positive in the true north direction, and uz is positive
%                       upward.
%
% Future Improvements:
%
%   [1] TODO Check the assumptions about azimuth and elevation frame of
%       reference are correct
%
% References:
%
%   [1] J F. Newman et al. Evaluation of three lidar scanning strategies for
%       turbulence measurements. Atmos. Meas. Tech., 9, 1993-2013, 2016

%
% Author:                   T. H. Clark
% Work address:             Octue Ltd
%                           Hauser Forum
%                           3 Charles Babbage Road
%                           Cambridge
%                           CB3 0GT
% Email:                    tom@octue.com
% Website:                  www.octue.com
%
% Copyright (c) 2018 Octue Ltd, All Rights Reserved.

% Sizes
nGroups = size(doppler,2);
nRangeGates = size(doppler,3);

% Precompute trig terms, ensuring column vector orientation
cosTheta = cosd(az(:));
sinTheta = sind(az(:));
sinPhi = sind(el(:));
cosPhi = cosd(el(:));

% Threshold the velocity values by confidence
% doppler(intensity < threshold) = NaN;

% Use a median based moving average to reduce impact of outliers on the
% mean calculation over the beam group
doppler = nanmean(smoothdata(doppler, 1, 'movmedian', 5, 'omitnan'), 1);

% Collapse the singleton first dimension. Can't squeeze as we need to retain
% orientation in the event of nGroups = 1.
doppler = reshape(doppler, [nGroups, nRangeGates]);

% Multiply for cartesian components
uxGroups = bsxfun(@times, doppler, cosPhi.*sinTheta);
uyGroups = bsxfun(@times, doppler, cosPhi.*cosTheta);
uzGroups = bsxfun(@times, doppler, sinPhi);

% TODO if we chose to rely on the statistics toolbox, this code collapses simply
% to nanmedian(siGroup, 2)...

% Preallocate outputs
ux = zeros(nRangeGates, 1);
uy = zeros(nRangeGates, 1);
uz = zeros(nRangeGates, 1);

% Median over the groups, excluding NaN values to maximise data retention
for iGate = 1:nRangeGates

    uxMask = ~isnan(uxGroups(:, iGate));
    ux(iGate) = median(uxGroups(uxMask, iGate));

    uyMask = ~isnan(uyGroups(:, iGate));
    uy(iGate) = median(uyGroups(uyMask, iGate));

    uzMask = ~isnan(uzGroups(:, iGate));
    uz(iGate) = median(uzGroups(uzMask, iGate));

end

% Fill missing values with a 5 point median to ensure robust output
if fillMissing
    u = fillmissing([ux, uy, uz], 'movmedian', 5);
else
    u = [ux uy uz];
end

% TODO Quality check on number of back filled points. Above a certain proportion
% of range gates, throw a warning or an error
