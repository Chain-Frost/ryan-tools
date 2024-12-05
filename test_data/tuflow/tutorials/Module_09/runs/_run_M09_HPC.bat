set exe="..\..\..\..\exe\2023-03-AF\TUFLOW_iSP_w64.exe"
set run=start "TUFLOW" /wait %exe% -b

%run% -e1 05p	-e2 1hr		M09_5m_~e1~_~e2~_001.tcf
%run% -e1 05p	-e2 2hr		M09_5m_~e1~_~e2~_001.tcf
%run% -e1 02p	-e2 1hr		M09_5m_~e1~_~e2~_001.tcf
%run% -e1 02p	-e2 2hr		M09_5m_~e1~_~e2~_001.tcf
%run% -e1 01p	-e2 1hr		M09_5m_~e1~_~e2~_001.tcf
%run% -e1 01p	-e2 2hr		M09_5m_~e1~_~e2~_001.tcf
