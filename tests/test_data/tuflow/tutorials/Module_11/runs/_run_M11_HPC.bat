set exe="..\..\..\..\exe\2023-03-AF\TUFLOW_iSP_w64.exe"
set run=start "TUFLOW" /wait %exe% -b

%run%	M11_5m_001.tcf
%run%	M11_5m_002.tcf

