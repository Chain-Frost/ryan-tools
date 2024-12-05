set exe="..\..\..\..\exe\2023-03-AF\TUFLOW_iSP_w64.exe"
set run=start "TUFLOW" /wait %exe% -b

%run% -s1 10m				M08_~s1~_001.tcf
%run% -s1 5m				M08_~s1~_001.tcf
%run% -s1 2.5m				M08_~s1~_001.tcf
%run% -s1 10m 	-s2 EXG		M08_~s1~_~s2~_002.tcf
%run% -s1 10m 	-s2 DEV		M08_~s1~_~s2~_002.tcf
%run% -s1 5m 	-s2 EXG		M08_~s1~_~s2~_002.tcf
%run% -s1 5m 	-s2 DEV		M08_~s1~_~s2~_002.tcf
%run% -s1 2.5m 	-s2 EXG		M08_~s1~_~s2~_002.tcf
%run% -s1 2.5m 	-s2 DEV		M08_~s1~_~s2~_002.tcf

