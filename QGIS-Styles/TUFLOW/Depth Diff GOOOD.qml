<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.38.2-Grenoble" styleCategories="Labeling">
  <pipe-data-defined-properties>
    <Option type="Map">
      <Option value="" name="name" type="QString"/>
      <Option name="properties"/>
      <Option value="collection" name="type" type="QString"/>
    </Option>
  </pipe-data-defined-properties>
  <pipe>
    <provider>
      <resampling maxOversampling="2" enabled="false" zoomedInResamplingMethod="nearestNeighbour" zoomedOutResamplingMethod="nearestNeighbour"/>
    </provider>
    <rasterrenderer classificationMax="2" nodataColor="" alphaBand="-1" classificationMin="-1" opacity="1" band="1" type="singlebandpseudocolor">
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
        <colorrampshader labelPrecision="2" minimumValue="-1" maximumValue="2" classificationMode="2" clip="0" colorRampType="DISCRETE">
          <colorramp name="[source]" type="gradient">
            <Option type="Map">
              <Option value="0,51,140,255,rgb:0,0.20000000000000001,0.5490196078431373,1" name="color1" type="QString"/>
              <Option value="0,0,0,255,rgb:0,0,0,1" name="color2" type="QString"/>
              <Option value="ccw" name="direction" type="QString"/>
              <Option value="0" name="discrete" type="QString"/>
              <Option value="gradient" name="rampType" type="QString"/>
              <Option value="rgb" name="spec" type="QString"/>
              <Option value="0.166667;12,130,234,255,rgb:0.04705882352941176,0.50980392156862742,0.91764705882352937,1;rgb;ccw:0.233333;9,241,253,255,rgb:0.03529411764705882,0.94509803921568625,0.99215686274509807,1;rgb;ccw:0.316667;1,255,166,255,rgb:0.00392156862745098,1,0.65098039215686276,1;rgb;ccw:0.35;255,255,255,0,hsv:0.09166666666666666,0,1,0;rgb;ccw:0.433333;255,230,7,255,rgb:1,0.90196078431372551,0.02745098039215686,1;rgb;ccw:0.5;254,148,0,255,rgb:0.99607843137254903,0.58039215686274515,0,1;rgb;ccw:0.666667;247,33,10,255,rgb:0.96862745098039216,0.12941176470588237,0.0392156862745098,1;rgb;ccw:1;146,18,24,255,rgb:0.5725490196078431,0.07058823529411765,0.09411764705882353,1;rgb;ccw" name="stops" type="QString"/>
            </Option>
          </colorramp>
          <item value="-1" label="&lt;= -1.00" alpha="255" color="#00338c"/>
          <item value="-0.5" label="-1.00 - -0.50" alpha="255" color="#0c82ea"/>
          <item value="-0.3" label="-0.50 - -0.30" alpha="255" color="#09f1fd"/>
          <item value="-0.05" label="-0.30 - -0.05" alpha="255" color="#01ffa6"/>
          <item value="0.05" label="-0.05 - 0.05" alpha="0" color="#ffffff"/>
          <item value="0.3" label="0.05 - 0.30" alpha="255" color="#ffe607"/>
          <item value="0.5" label="0.30 - 0.50" alpha="255" color="#fe9400"/>
          <item value="1" label="0.50 - 1.00" alpha="255" color="#f7210a"/>
          <item value="2" label="1.00 - 2.00" alpha="255" color="#921218"/>
          <item value="inf" label="> 2.00" alpha="255" color="#000000"/>
          <rampLegendSettings minimumLabel="" direction="0" suffix="" prefix="" useContinuousLegend="1" orientation="2" maximumLabel="">
            <numericFormat id="basic">
              <Option type="Map">
                <Option name="decimal_separator" type="invalid"/>
                <Option value="6" name="decimals" type="int"/>
                <Option value="0" name="rounding_type" type="int"/>
                <Option value="false" name="show_plus" type="bool"/>
                <Option value="true" name="show_thousand_separator" type="bool"/>
                <Option value="false" name="show_trailing_zeros" type="bool"/>
                <Option name="thousand_separator" type="invalid"/>
              </Option>
            </numericFormat>
          </rampLegendSettings>
        </colorrampshader>
      </rastershader>
    </rasterrenderer>
    <brightnesscontrast contrast="0" brightness="0" gamma="1"/>
    <huesaturation colorizeBlue="128" grayscaleMode="0" colorizeGreen="128" invertColors="0" colorizeRed="255" colorizeStrength="100" saturation="0" colorizeOn="0"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
