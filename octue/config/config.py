from ..exceptions import NotImplementedYet


def write_config(s, file):
    """Writes a config file to create example for input to an application
    %

    % Check the input is a structure
    if ~isstruct(s)
        error('Input s must be a structure containing named configuration parameters.')
    end

    % Save to default filename
    if nargin == 1
        file = 'config.json';
    end

    % Encode the config to a string
    str = jsonencode(s);

    % Open a file for writing text (discarding existing contents if any), print to it, close it
    fileId = fopen(file, 'wt');
    if fileId == -1
        error(['Unable to open file for writing: ' file])
    end
    nBytes = fprintf(fileId, '%s', str);
    status = fclose(fileId);
    assert(status==0, ['Unable to write config file: ' file])
    fprintf('Wrote %i bytes to file %s\n', nBytes, file)

    end

    """
    raise NotImplementedYet()
