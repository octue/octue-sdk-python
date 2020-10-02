function vad_adem_reduce(~, intermValsIter, outKVStore)
%VAD_ADEM_REDUCE

maxTime = -inf;
while hasnext(intermValsIter)
    maxTime = max(maxTime, getnext(intermValsIter));
end
add(outKVStore, 'MaxTime', maxTime);

end
