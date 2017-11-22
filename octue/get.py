from .exceptions import NotImplementedYet


def get(thing_name, set_analysis=None):
    """
    %GET Gets the current analysis (or named property of the current analysis)

    persistent analysis;

    if ~ischar(thingName)

        error('octue.get() method must be called with a string name of the object you wish to retrieve')

    elseif strcmpi(thingName, '--reset') || strcmpi(thingName, '-reset') || strcmpi(thingName, '-r')
        %Reset the analysis
        analysis = [];

    elseif ~isempty(analysis)
        % Get the analysis property

        if nargin > 1
            error('Persistent analysis already registered. You cannot reset or duplicate the analysis once registered. Use ocute.clear() to remove the currently registered analysis if you wish to start fresh.')
        end
        switch lower(thingName)
            case 'analysis'
                thing = analysis;
            case 'config'
                thing = analysis.Config;
            case 'inputdir'
                thing = analysis.InputDir;
            case 'outputdir'
                thing = analysis.OutputDir;
            case 'tmpdir'
                thing = analysis.TmpDir;
            case 'logdir'
                thing = analysis.LogDir;
            case 'logger'
                thing = analysis.Logger;
            case 'inputmanifest'
                thing = analysis.InputManifest;
            case 'outputmanifest'
                thing = analysis.OutputManifest;
            otherwise
                error(['Invalid or unknown name ' thingName ' passed to the octue.get() function.'])
        end

    elseif nargin > 1 && strcmpi(thingName, 'analysis')
        if isa(setAnalysis, 'octue.Analysis')
            analysis = setAnalysis;
        else
            error('Cannot set persistent analysis to a non-Analysis class object')
        end

    else

        error('get() must be called either as get(thingName) or (only to initialise) get(''analysis'', analysis)')

    end

    end
    """
    raise NotImplementedYet()
