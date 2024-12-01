<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis maxScale="0" styleCategories="AllStyleCategories" hasScaleBasedVisibilityFlag="0" version="3.22.6-Białowieża" minScale="1e+08">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>0</Searchable>
    <Private>0</Private>
  </flags>
  <temporal mode="0" fetchMode="0" enabled="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <customproperties>
    <Option type="Map">
      <Option name="WMSBackgroundLayer" type="bool" value="false"/>
      <Option name="WMSPublishDataSourceUrl" type="bool" value="false"/>
      <Option name="embeddedWidgets/count" type="int" value="0"/>
    </Option>
  </customproperties>
  <pipe-data-defined-properties>
    <Option type="Map">
      <Option name="name" type="QString" value=""/>
      <Option name="properties"/>
      <Option name="type" type="QString" value="collection"/>
    </Option>
  </pipe-data-defined-properties>
  <pipe>
    <provider>
      <resampling zoomedOutResamplingMethod="nearestNeighbour" zoomedInResamplingMethod="nearestNeighbour" enabled="false" maxOversampling="2"/>
    </provider>
    <rasterrenderer classificationMax="2.5" band="1" nodataColor="" type="singlebandpseudocolor" opacity="1" alphaBand="-1" classificationMin="1">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <rastershader>
        <colorrampshader classificationMode="2" minimumValue="1" colorRampType="DISCRETE" clip="0" labelPrecision="2" maximumValue="2.5">
          <colorramp name="[source]" type="gradient">
            <Option type="Map">
              <Option name="color1" type="QString" value="69,91,205,255"/>
              <Option name="color2" type="QString" value="122,4,3,255"/>
              <Option name="discrete" type="QString" value="0"/>
              <Option name="rampType" type="QString" value="gradient"/>
              <Option name="stops" type="QString" value="-0.333333;62,156,254,255:0.666667;24,215,203,255:1.06667;72,248,130,255:1.26667;164,252,60,255:1.93333;226,220,56,255:2.33333;254,163,49,255:2.73333;239,89,17,255:3.13333;194,36,3,255"/>
            </Option>
            <prop v="69,91,205,255" k="color1"/>
            <prop v="122,4,3,255" k="color2"/>
            <prop v="0" k="discrete"/>
            <prop v="gradient" k="rampType"/>
            <prop v="-0.333333;62,156,254,255:0.666667;24,215,203,255:1.06667;72,248,130,255:1.26667;164,252,60,255:1.93333;226,220,56,255:2.33333;254,163,49,255:2.73333;239,89,17,255:3.13333;194,36,3,255" k="stops"/>
          </colorramp>
          <item alpha="255" label="&lt;= 0.20- None" value="0.2" color="#455bcd"/>
          <item alpha="255" label="0.20 - 0.50- None" value="0.5" color="#3e9cfe"/>
          <item alpha="255" label="0.50 - 2.00- None" value="2" color="#18d7cb"/>
          <item alpha="255" label="2.00 - 2.60- Facing" value="2.6" color="#48f882"/>
          <item alpha="255" label="2.60 - 2.90- Light" value="2.9" color="#a4fc3c"/>
          <item alpha="255" label="2.90 - 3.90- 1/4T" value="3.9" color="#e2dc38"/>
          <item alpha="255" label="3.90 - 4.50- 1/2T" value="4.5" color="#fea331"/>
          <item alpha="255" label="4.50 - 5.10- 1T" value="5.1" color="#ef5911"/>
          <item alpha="255" label="5.10 - 5.70- 2T" value="5.7" color="#c22403"/>
          <item alpha="255" label="5.70 - 6.40- 4T" value="6.4" color="#7a0403"/>
          <rampLegendSettings orientation="2" suffix="" minimumLabel="" maximumLabel="" prefix="" useContinuousLegend="1" direction="0">
            <numericFormat id="basic">
              <Option type="Map">
                <Option name="decimal_separator" type="QChar" value=""/>
                <Option name="decimals" type="int" value="6"/>
                <Option name="rounding_type" type="int" value="0"/>
                <Option name="show_plus" type="bool" value="false"/>
                <Option name="show_thousand_separator" type="bool" value="true"/>
                <Option name="show_trailing_zeros" type="bool" value="false"/>
                <Option name="thousand_separator" type="QChar" value=""/>
              </Option>
            </numericFormat>
          </rampLegendSettings>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast gamma="1" contrast="0" brightness="0"/>
    <huesaturation colorizeStrength="100" grayscaleMode="0" colorizeBlue="128" colorizeRed="255" saturation="0" invertColors="0" colorizeGreen="128" colorizeOn="0"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
