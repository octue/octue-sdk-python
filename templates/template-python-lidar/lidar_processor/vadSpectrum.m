function s = vadSpectrum(az, el, spectrum)
%VAD Computes VAD from multiple beams using Velocity Azimuth Display method
%
% Syntax:
%
%   u = VAD(az, el, doppler, intensity) Computes velocity from a series of beams
%   at some azimuth and elevation.
%
% Inputs:
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
%       spectrum        [nPoints x nGroups x nRangeGates]
%
%                       Contains an n-point power spectrum.
%
% Outputs:
%
%       s               [nPoints x 3 x nRangeGates]
%                       Array containing [S1(:) S2(:) S3(:)] for each range gate
%                       where direction 1 is positive in the true East
%                       direction, direction 2 is positive in the true north
%                       direction, and direction 3 is positive upward.
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

% Precompute trig terms, ensuring row vector orientation
cosTheta = cosd(az(:)');
sinTheta = sind(az(:)');
sinPhi = sind(el(:)');
cosPhi = cosd(el(:)');

% Multiply for cartesian components
s1Groups = bsxfun(@times, spectrum, cosPhi.*sinTheta);
s2Groups = bsxfun(@times, spectrum, cosPhi.*cosTheta);
s3Groups = bsxfun(@times, spectrum, sinPhi);

% It's absolute power, so sum of squares
s1 = sqrt(sum(s1Groups.^2, 2));
s2 = sqrt(sum(s2Groups.^2, 2));
s3 = sqrt(sum(s3Groups.^2, 2));

% Output
s = cat(2, s1, s2, s3);
