function [profile, fit, sProfile, st] = lidar_adem(z, u, spectrum, f, fTarget)
%LIDAR_ADEM Applies the ADEM technique to VAD results from a grouped-beam scan.
%
% Syntax:
%
%   [p, st] = LIDAR_ADEM(az, el, doppler, intensity)
%
% Inputs:
%
%       z               [nRangeGates x 1]
%
%                       Heights in metres for which the
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



%       z           [nZ x 1]    Height in m above the wall at which boundary
%                               layer profile was measured. Ascending but not
%                               strictly monotonic.
%
%       Ux          [nZ x 1]    Streamwise velocity in m/s at points
%                               corresponding to the heights in z
%
%       weights     [nZ x 1]    Relative confidence weightings ranging between 0
%                               and 1 for the Ux measurements at corresponding
%                               heights. 0 = no confidence (likely just noise)
%                               while 1 = best confidence. Leave empty for no
%                               weightings (set to all 1s).
%
%       x0          [4 x 1]     Initial guesses for [Pi, S0, deltac0, U10].

% Quick check
if (size(z, 1) ~= size(u,1)) || (size(z, 2) ~= 1) || (size(u, 2) ~= 3)
    error('Inputs z and u must be of size [nRangeGates x 1] and [nRangeGates x 3] respectively.')
end

% Assumption: We use the horizontal velocity magnitude to fit, not the ux, uy
% components.
uh = sqrt(sum(u(:, 1:2).^2, 2));

% TODO improved initial guess
x0 = [0.75, 10, 1000, max(uh)];

% Constrain the boundary layer height
constrained = [0 0 1 0];

% Use the lewkowicz bl model
type = 'lewkowicz';

% TODO improved weighting (automatic) and remove this masking botch
warning('Applying hard-coded weighting of 0 to first three height bins. Contact developers to resolve.')
weights = [];
profile = fitMeanProfile(z(4:end), uh(4:end), weights, x0, constrained, type);

% Store a structure with the numeric results in it for easy saving/plotting
fit.z = z;
fit.uh =  getMeanProfile(profile, z);
fit.constrained = constrained;
fit.weights = weights;
fit.x0 = x0;
fit.type = type;

% Run adem for the 6 parameters
beta = 0;
zeta = 0;
sProfile = adem(profile.deltac, profile.U1, profile.Pi, profile.S, beta, zeta)



end
