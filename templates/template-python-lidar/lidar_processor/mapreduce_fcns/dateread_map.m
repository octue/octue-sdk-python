function [outputArg1,outputArg2] = dateread_map(data, info, intermKV)
%DATEREAD_MAP


datestamp = data.RayTime;

posixTime = datenum(datestamp);

keys = {'PosixTime'};

addmulti(intermKV, keys, posixTime)

end
