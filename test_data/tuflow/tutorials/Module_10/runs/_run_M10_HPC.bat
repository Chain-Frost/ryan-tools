set exe="..\..\..\..\exe\2023-03-AF\TUFLOW_iSP_w64.exe"
set run=start "TUFLOW" /wait %exe% -b

%run%	M10_5m_001.tcf
%run%	M10_5m_002.tcf
%run%	M10_5m_003.tcf
%run%	M10_5m_004.tcf
%run%	M10_5m_005.tcf
