# Spatial_Rainfall_Pattern – QGIS 3.44.3
# Per-cell grid (no dissolve) → PoS raster sampling → NoData filter → clip in raster CRS
# → area-weighted average → add f1,f2 on clipped cells
# → per-vertex reprojection + mm snap (to input polygon CRS)
# → topology rebuild → PoS attribute restore
# → final inward buffer (−0.5 m, miter) → drop <1 m² → singleparts
# → write sink in input polygon CRS + print summary (avg_rainfall | centroid_raster_value)
# 2025-10-29 version
from __future__ import annotations
from typing import Any

from qgis.core import (
    QgsFeature, QgsField, QgsFields, QgsGeometry, QgsPointXY,
    QgsProcessing, QgsProcessingAlgorithm, QgsProcessingContext,
    QgsProcessingException, QgsProcessingFeedback, QgsProcessingMultiStepFeedback,
    QgsProcessingParameterFeatureSink, QgsProcessingParameterRasterLayer,
    QgsProcessingParameterVectorLayer, QgsProject, QgsVectorLayer, QgsWkbTypes,
    QgsDistanceArea, QgsFeatureSink, QgsCoordinateTransform,
)
from qgis.PyQt.QtCore import QVariant
from qgis import processing


class Spatial_rainfall_pattern(QgsProcessingAlgorithm):
    PARAM_CATCHMENT = "catchment"
    PARAM_RASTER = "rain_raster"
    PARAM_OUT = "Rainfallboxes"

    PRECISION_GRID_M = 0.001   # snap grid in final CRS (m)
    FINAL_BUFFER_M   = -0.1    # inward buffer (m), miter joins

    def initAlgorithm(self, config: dict[str, Any] | None = None) -> None:
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.PARAM_CATCHMENT, "Catchment Extent (polygon)",
                types=[QgsProcessing.TypeVectorPolygon],
            )
        )
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.PARAM_RASTER, "Rainfall Raster (GeoTIFF)"
            )
        )
        # Sink is created only at the end (in input polygon CRS)
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.PARAM_OUT, "rainfall-boxes",
                type=QgsProcessing.TypeVectorAnyGeometry, createByDefault=True,
            )
        )

    # ---------- helpers ----------
    @staticmethod
    def _round_to_grid(v: float, g: float) -> float:
        return round(v / g) * g

    @classmethod
    def _snap_xy(cls, x: float, y: float, grid: float) -> QgsPointXY:
        return QgsPointXY(cls._round_to_grid(x, grid), cls._round_to_grid(y, grid))

    @classmethod
    def _close_if_needed(cls, ring: list[QgsPointXY]) -> None:
        if ring and (ring[0].x() != ring[-1].x() or ring[0].y() != ring[-1].y()):
            ring.append(QgsPointXY(ring[0]))

    @staticmethod
    def _safe_float(val) -> float:
        try:
            return float(val)
        except Exception:
            return float("nan")

    def _repair_geoms_no_makevalid(self, input_id: str, context, feedback) -> str:
        try:
            return processing.run(
                "native:fixgeometries",
                {"INPUT": input_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
                context=context, feedback=feedback, is_child_algorithm=True,
            )["OUTPUT"]
        except Exception:
            pass
        try:
            return processing.run(
                "native:buffer",
                {"INPUT": input_id, "DISTANCE": 0.0, "SEGMENTS": 5,
                 "END_CAP_STYLE": 0, "JOIN_STYLE": 0, "MITER_LIMIT": 2.0,
                 "DISSOLVE": False, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
                context=context, feedback=feedback, is_child_algorithm=True,
            )["OUTPUT"]
        except Exception as e:
            raise QgsProcessingException(f"Geometry repair failed (fixgeometries/buffer 0). Error: {e}")

    def _transform_snap_polygons(
        self, input_id: str, src_crs, dst_crs, grid: float,
        context: QgsProcessingContext, feedback: QgsProcessingFeedback
    ) -> str:
        """Per-vertex transform (src→dst), snap at build; expects Polygon singleparts; preserves attributes."""
        src = context.getMapLayer(input_id)
        if src is None:
            raise QgsProcessingException("Transform: failed to resolve layer.")
        if QgsWkbTypes.geometryType(src.wkbType()) != QgsWkbTypes.PolygonGeometry:
            raise QgsProcessingException("Transform expects polygonal input.")

        uri = f"Polygon?crs={dst_crs.authid()}"
        if QgsWkbTypes.hasZ(src.wkbType()): uri += "&zDimension=1"
        if QgsWkbTypes.hasM(src.wkbType()): uri += "&mDimension=1"
        mem = QgsVectorLayer(uri, "reproj_snapped", "memory")
        if not mem.isValid():
            raise QgsProcessingException("Transform: failed to create memory layer.")
        dp = mem.dataProvider(); dp.addAttributes(src.fields()); mem.updateFields()

        tf = QgsCoordinateTransform(src_crs, dst_crs, context.transformContext())

        def tx_ring(ring_coords) -> list[QgsPointXY]:
            out = []; last = None
            for p in ring_coords:
                pt = tf.transform(p.x(), p.y())
                sp = self._snap_xy(pt.x(), pt.y(), grid)
                if last is None or (sp.x() != last.x() or sp.y() != last.y()):
                    out.append(sp); last = sp
            self._close_if_needed(out); return out

        total = max(1, src.featureCount()); batch: list[QgsFeature] = []
        for i, f in enumerate(src.getFeatures()):
            if feedback.isCanceled(): break
            g = f.geometry()
            if not g or g.isEmpty(): continue
            poly = g.asPolygon()
            if not poly:
                g = g.buffer(0, 5); poly = g.asPolygon()
                if not poly: continue
            rebuilt: list[list[QgsPointXY]] = []
            for ring in poly:
                xy = [QgsPointXY(p.x(), p.y()) for p in ring]
                rebuilt.append(tx_ring(xy))
            out_geom = QgsGeometry.fromPolygonXY(rebuilt)
            nf = QgsFeature(mem.fields()); nf.setGeometry(out_geom); nf.setAttributes(f.attributes()); batch.append(nf)
            if len(batch) >= 5000:
                dp.addFeatures(batch); batch.clear()
                feedback.setProgress(int(100 * (i + 1) / total))
        if batch: dp.addFeatures(batch)
        context.temporaryLayerStore().addMapLayer(mem)
        return mem.id()

    # ---------- main ----------
    def processAlgorithm(self, parameters: dict[str, Any],
                         context: QgsProcessingContext,
                         model_feedback: QgsProcessingFeedback) -> dict[str, Any]:
        fb = QgsProcessingMultiStepFeedback(60, model_feedback)
        results: dict[str, Any] = {}

        catchment = self.parameterAsVectorLayer(parameters, self.PARAM_CATCHMENT, context)
        raster = self.parameterAsRasterLayer(parameters, self.PARAM_RASTER, context)
        if catchment is None or raster is None:
            raise QgsProcessingException("Missing input layer(s).")

        target_crs = catchment.crs()   # final/output CRS (projected; metres)
        raster_crs = raster.crs()      # working CRS

        # A) Catchment → raster CRS; repair; index
        cat_r_id = processing.run(
            "native:reprojectlayer",
            {"INPUT": catchment, "TARGET_CRS": raster_crs, "OPERATION": "",
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(1)

        cat_r_id = self._repair_geoms_no_makevalid(cat_r_id, context, fb); fb.setCurrentStep(2)
        processing.run("native:createspatialindex", {"INPUT": cat_r_id},
                       context=context, feedback=fb, is_child_algorithm=True)

        # A2) Centroid PoS of dissolved catchment (for DN at centroid)
        cat_diss_id = processing.run(
            "native:dissolve",
            {"INPUT": cat_r_id, "FIELD": [], "SEPARATE_DISJOINT": False,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(3)

        cat_centroid_pt = processing.run(
            "native:pointonsurface",
            {"INPUT": cat_diss_id, "ALL_PARTS": False, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(4)

        # B) Per-cell grid aligned to raster envelope
        extent = raster.extent()
        px = abs(raster.rasterUnitsPerPixelX()); py = abs(raster.rasterUnitsPerPixelY())

        grid_id = processing.run(
            "native:creategrid",
            {"TYPE": 2, "EXTENT": extent, "HSPACING": px, "VSPACING": py,
             "HOVERLAY": 0, "VOVERLAY": 0, "CRS": raster_crs,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(5)

        grid_uid = processing.run(
            "native:addautoincrementalfield",
            {"INPUT": grid_id, "FIELD_NAME": "UID", "START": 1, "GROUP_FIELDS": [], "MODULUS": 0,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(6)

        processing.run("native:createspatialindex", {"INPUT": grid_uid},
                       context=context, feedback=fb, is_child_algorithm=True)

        pts_id = processing.run(
            "native:pointonsurface",
            {"INPUT": grid_uid, "ALL_PARTS": False, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(7)

        # DN sampling (and NoData detection)
        rp = raster.dataProvider()
        no_data_vals = rp.srcNoDataValueList(1) if hasattr(rp, "srcNoDataValueList") else []

        sampled_pts = processing.run(
            "native:rastersampling",
            {"INPUT": pts_id, "RASTERCOPY": raster, "COLUMN_PREFIX": "DN_",
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(8)

        centroid_sample = processing.run(
            "native:rastersampling",
            {"INPUT": cat_centroid_pt, "RASTERCOPY": raster, "COLUMN_PREFIX": "DN_",
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(9)

        grid_with_dn = processing.run(
            "native:joinattributestable",
            {"INPUT": grid_uid, "FIELD": "UID",
             "INPUT_2": sampled_pts, "FIELD_2": "UID",
             "FIELDS_TO_COPY": [], "METHOD": 1, "DISCARD_NONMATCHING": False,
             "PREFIX": "", "NON_MATCHING": QgsProcessing.TEMPORARY_OUTPUT,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(10)

        grid_layer = context.getMapLayer(grid_with_dn)
        dn_field = next((f.name() for f in grid_layer.fields() if f.name().lower().startswith("dn_")), None)
        if not dn_field:
            raise QgsProcessingException("Raster sampling failed: DN_* field not found.")

        expr = f"\"{dn_field}\" IS NOT NULL"
        if no_data_vals:
            expr += " AND " + " AND ".join([f"\"{dn_field}\" <> {v}" for v in no_data_vals])

        valid_cells = processing.run(
            "native:extractbyexpression",
            {"INPUT": grid_with_dn, "EXPRESSION": expr,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(11)

        processing.run("native:createspatialindex", {"INPUT": valid_cells},
                       context=context, feedback=fb, is_child_algorithm=True)

        # Clip per-cell polygons to catchment (raster CRS)
        clipped_id = processing.run(
            "native:clip",
            {"INPUT": valid_cells, "OVERLAY": cat_r_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(12)

        # Area-weighted average DN (raster CRS)
        clipped = context.getMapLayer(clipped_id)
        d = QgsDistanceArea(); d.setEllipsoid(QgsProject.instance().ellipsoid())
        d.setSourceCrs(clipped.crs(), context.transformContext())
        sum_area = 0.0; sum_area_dn = 0.0
        for ft in clipped.getFeatures():
            g = ft.geometry()
            if not g or g.isEmpty(): continue
            dn = self._safe_float(ft[dn_field]); a = d.measureArea(g)
            sum_area += a; sum_area_dn += a * dn
        if sum_area == 0.0:
            raise QgsProcessingException("Total area is zero after clipping.")
        avg_rainfall = sum_area_dn / sum_area; fb.setCurrentStep(13)

        # DN at catchment centroid
        centroid_layer = context.getMapLayer(centroid_sample)
        centroid_dn_field = next((f.name() for f in centroid_layer.fields() if f.name().lower().startswith("dn_")), None)
        if not centroid_dn_field:
            raise QgsProcessingException("Centroid raster sampling failed: DN_* field not found.")
        centroid_dn_val = None
        for f in centroid_layer.getFeatures():
            centroid_dn_val = self._safe_float(f[centroid_dn_field]); break
        if centroid_dn_val is None:
            centroid_dn_val = float("nan")

        # --- Add f1 and f2 on the clipped polygons (raster CRS) so attributes carry forward
        clipped_with_f1 = processing.run(
            "native:fieldcalculator",
            {
                "INPUT": clipped_id,
                "FIELD_NAME": "f1",
                "FIELD_TYPE": 0,           # Double
                "FIELD_LENGTH": 20,
                "FIELD_PRECISION": 8,
                "NEW_FIELD": True,
                "FORMULA": f'"{dn_field}" / {avg_rainfall}',
                "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
            },
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        clipped_with_f1_f2 = processing.run(
            "native:fieldcalculator",
            {
                "INPUT": clipped_with_f1,
                "FIELD_NAME": "f2",
                "FIELD_TYPE": 0,           # Double
                "FIELD_LENGTH": 10,
                "FIELD_PRECISION": 2,
                "NEW_FIELD": True,
                "FORMULA": "1.0",
                "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
            },
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        # Continue from this layer
        clipped_id = clipped_with_f1_f2

        # Enforce singleparts before transform
        single_work = processing.run(
            "native:multiparttosingleparts",
            {"INPUT": clipped_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(14)

        # Per-vertex reprojection + snap (raster CRS → target CRS)
        fb.pushInfo(f"Per-vertex transform {clipped.crs().authid()} → {target_crs.authid()} with grid={self.PRECISION_GRID_M} m…")
        rebuilt_id = self._transform_snap_polygons(
            input_id=single_work, src_crs=clipped.crs(), dst_crs=target_crs,
            grid=self.PRECISION_GRID_M, context=context, feedback=fb,
        ); fb.setCurrentStep(15)

        # Rebuild topology (polygons→lines→dissolve→polygonize)
        lines_id = processing.run(
            "native:polygonstolines",
            {"INPUT": rebuilt_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]
        dissolved_edges = processing.run(
            "native:dissolve",
            {"INPUT": lines_id, "FIELD": [], "SEPARATE_DISJOINT": False,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]
        final_tiles = processing.run(
            "native:polygonize",
            {"INPUT": dissolved_edges, "KEEP_FIELDS": False,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(16)

        # Attribute restore via PoS (avoids 0-distance tie issues)
        final_uid = processing.run(
            "native:addautoincrementalfield",
            {"INPUT": final_tiles, "FIELD_NAME": "UID", "START": 1, "GROUP_FIELDS": [], "MODULUS": 0,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]
        pts_final = processing.run(
            "native:pointonsurface",
            {"INPUT": final_uid, "ALL_PARTS": False, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        processing.run("native:createspatialindex", {"INPUT": pts_final},
                       context=context, feedback=fb, is_child_algorithm=True)
        processing.run("native:createspatialindex", {"INPUT": rebuilt_id},
                       context=context, feedback=fb, is_child_algorithm=True)

        def _nearest_join_once(max_dist: float) -> tuple[str, int]:
            j = processing.run(
                "native:joinbynearest",
                {"INPUT": pts_final, "INPUT_2": rebuilt_id, "FIELDS_TO_COPY": [],  # copy ALL fields (DN_*, f1, f2, …)
                 "DISCARD_NONMATCHING": False, "MAX_DISTANCE": max_dist,
                 "NEIGHBORS": 1, "PREFIX": "", "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
                context=context, feedback=fb, is_child_algorithm=True,
            )
            pts_n = context.getMapLayer(pts_final).featureCount()
            join_n = context.getMapLayer(j["OUTPUT"]).featureCount()
            unjoined = max(0, pts_n - join_n)
            return j["OUTPUT"], unjoined

        tol = max(0.2, 2 * self.PRECISION_GRID_M)
        pts_joined, un1 = _nearest_join_once(tol)
        if un1 > 0:
            pts_joined, un2 = _nearest_join_once(tol + 0.005)
            if un2 > 0:
                fb.pushInfo(f"Warning: {un2} point(s) could not be matched after tolerance increase.")

        polys_joined = processing.run(
            "native:joinattributestable",
            {"INPUT": final_uid, "FIELD": "UID", "INPUT_2": pts_joined, "FIELD_2": "UID",
             "FIELDS_TO_COPY": [], "METHOD": 1, "DISCARD_NONMATCHING": False, "PREFIX": "",
             "NON_MATCHING": QgsProcessing.TEMPORARY_OUTPUT, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]; fb.setCurrentStep(17)

        # Clean: dedupe → singleparts → fix
        unique_id = processing.run(
            "native:deleteduplicategeometries",
            {"INPUT": polys_joined, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]
        single_id = processing.run(
            "native:multiparttosingleparts",
            {"INPUT": unique_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]
        cleaned_id = processing.run(
            "native:fixgeometries",
            {"INPUT": single_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]
        processing.run("native:createspatialindex", {"INPUT": cleaned_id},
                       context=context, feedback=fb, is_child_algorithm=True)

        # FINAL: inward buffer (miter/square corners) to create separation
        buffered_id = processing.run(
            "native:buffer",
            {
                "INPUT": cleaned_id,
                "DISTANCE": self.FINAL_BUFFER_M,    # −0.5 m
                "SEGMENTS": 1,
                "END_CAP_STYLE": 0,
                "JOIN_STYLE": 1,                    # miter (square)
                "MITER_LIMIT": 10.0,
                "DISSOLVE": False,
                "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
            },
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        # Post-buffer: singleparts → fix → drop <1 m² → index
        post_single = processing.run(
            "native:multiparttosingleparts",
            {"INPUT": buffered_id, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        post_fixed = processing.run(
            "native:fixgeometries",
            {"INPUT": post_single, "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        post_kept = processing.run(
            "native:extractbyexpression",
            {"INPUT": post_fixed, "EXPRESSION": "$area >= 1.0",
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        before_cnt = context.getMapLayer(post_fixed).featureCount()
        after_cnt  = context.getMapLayer(post_kept).featureCount()
        removed_cnt = max(0, before_cnt - after_cnt)
        fb.pushInfo(f"[Final buffer] Removed {removed_cnt} feature(s) with area < 1 m².")
        processing.run("native:createspatialindex", {"INPUT": post_kept},
                       context=context, feedback=fb, is_child_algorithm=True)

        # QA (planar area parity) after buffer — non-zero due to shrink
        qa_diss = processing.run(
            "native:dissolve",
            {"INPUT": post_kept, "FIELD": [], "SEPARATE_DISJOINT": False,
             "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
            context=context, feedback=fb, is_child_algorithm=True,
        )["OUTPUT"]

        dqa = QgsDistanceArea(); dqa.setEllipsoid(""); dqa.setSourceCrs(target_crs, context.transformContext())
        parts_layer = context.getMapLayer(post_kept)
        diss_layer  = context.getMapLayer(qa_diss)
        sum_parts = sum(dqa.measureArea(f.geometry()) for f in parts_layer.getFeatures()
                        if f.geometry() and not f.geometry().isEmpty())
        sum_diss  = sum(dqa.measureArea(f.geometry()) for f in diss_layer.getFeatures()
                        if f.geometry() and not f.geometry().isEmpty())
        area_diff = abs(sum_parts - sum_diss)

        # ----- Write final sink in input polygon CRS -----
        layer_to_write_id = post_kept
        post_layer = context.getMapLayer(post_kept)
        if post_layer is None:
            raise QgsProcessingException("Final layer not found in context.")
        if post_layer.crs() != target_crs:
            layer_to_write_id = processing.run(
                "native:reprojectlayer",
                {"INPUT": post_kept, "TARGET_CRS": target_crs, "OPERATION": "",
                 "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT},
                context=context, feedback=fb, is_child_algorithm=True,
            )["OUTPUT"]
            post_layer = context.getMapLayer(layer_to_write_id)

        sink, sink_id = self.parameterAsSink(
            parameters, self.PARAM_OUT, context,
            post_layer.fields(), post_layer.wkbType(), target_crs
        )
        if sink is None:
            raise QgsProcessingException("Failed to create final output sink.")
        for f in post_layer.getFeatures():
            sink.addFeature(f, QgsFeatureSink.FastInsert)

        fb.pushInfo(f"[Output] Written in CRS={target_crs.authid()}  features={post_layer.featureCount()}")
        fb.pushInfo(f"=== Spatial_Rainfall_Pattern summary === avg_rainfall={avg_rainfall:.4f} | centroid_raster_value={centroid_dn_val:.4f}")

        # Return
        results[self.PARAM_OUT] = sink_id
        results["avg_rainfall"] = float(avg_rainfall)
        results["centroid_raster_value"] = float(centroid_dn_val)
        results["final_buffer_m"] = float(self.FINAL_BUFFER_M)
        results["removed_lt_1m2"] = int(removed_cnt)
        results["area_parity_diff"] = float(area_diff)
        results["deliverable_crs"] = target_crs.authid()
        return results

    def name(self) -> str: return "Spatial_Rainfall_Pattern"
    def displayName(self) -> str: return "Spatial_Rainfall_Pattern"
    def group(self) -> str: return ""
    def groupId(self) -> str: return ""
    def createInstance(self): return self.__class__()
