from dataclasses import dataclass

from include.utils.snowflake import Column


@dataclass
class Config:
    table: str
    column_list: list
    version: str
    database: str = "lake"
    schema: str = "aem"

    @property
    def s3_prefix(self):
        return f"lake/{self.database}.{self.schema}.{self.table}/{self.version}"


metadata_config = Config(
    table="asset_metadata",
    column_list=[
        Column("path", "VARCHAR", source_name="path", uniqueness=True),
        Column("model_id", "VARCHAR", source_name="modelId"),
        Column("jcrprimary_type", "VARCHAR", source_name="jcrprimaryType"),
        Column("jcrmixin_types", "VARCHAR", source_name="jcrmixinTypes"),
        Column("shoot_date", "TIMESTAMP_LTZ(3)", source_name="shootDate"),
        Column("shoot_type", "VARCHAR", source_name="shootType"),
        Column("ps_aux_lens_info", "VARCHAR", source_name="psAuxLensInfo"),
        Column("jcr_title", "VARCHAR", source_name="jcrTitle"),
        Column("ps_aux_serial_number", "VARCHAR", source_name="psAuxSerialNumber"),
        Column(
            "dam_physicalheightininches",
            "VARCHAR",
            source_name="damPhysicalheightininches",
        ),
        Column("tiff_compression", "VARCHAR", source_name="tiffCompression"),
        Column("tiff_make", "VARCHAR", source_name="tiffMake"),
        Column(
            "dam_physicalwidthininches",
            "VARCHAR",
            source_name="damPhysicalwidthininches",
        ),
        Column(
            "exif_exlens_specification",
            "VARCHAR",
            source_name="exifEXLensSpecification",
        ),
        Column(
            "exif_exphotographic_sensitivity",
            "VARCHAR",
            source_name="exifEXPhotographicSensitivity",
        ),
        Column("exif_custom_rendered", "VARCHAR", source_name="exifCustomRendered"),
        Column(
            "photoshop_date_created",
            "TIMESTAMP_LTZ(3)",
            source_name="photoshopDateCreated",
        ),
        Column("exif_scene_type", "NUMBER", source_name="exifSceneType"),
        Column("dam_fileformat", "VARCHAR", source_name="damFileformat"),
        Column("dam_progressive", "VARCHAR", source_name="damProgressive"),
        Column("exif_fnumber", "VARCHAR", source_name="exifFNumber"),
        Column(
            "tiff_planar_configuration",
            "VARCHAR",
            source_name="tiffPlanarConfiguration",
        ),
        Column("first_name", "VARCHAR", source_name="firstName"),
        Column("last_name", "VARCHAR", source_name="lastName"),
        Column("tiff_image_length", "NUMBER", source_name="tiffImageLength"),
        Column("tiff_yresolution", "VARCHAR", source_name="tiffYResolution"),
        Column(
            "exif_shutter_speed_value", "VARCHAR", source_name="exifShutterSpeedValue"
        ),
        Column("exif_focal_length", "VARCHAR", source_name="exifFocalLength"),
        Column("exif_color_space", "NUMBER", source_name="exifColorSpace"),
        Column("xmp_creator_tool", "VARCHAR", source_name="xmpCreatorTool"),
        Column(
            "photoshop_instructions", "VARCHAR", source_name="photoshopInstructions"
        ),
        Column(
            "exif_exlens_serial_number", "VARCHAR", source_name="exifEXLensSerialNumber"
        ),
        Column("dam_extracted", "VARCHAR", source_name="damextracted"),
        Column("dam_time_created", "VARCHAR", source_name="damTimeCreated"),
        Column("dc_format", "VARCHAR", source_name="dcFormat"),
        Column(
            "exif_focal_plane_xresolution",
            "VARCHAR",
            source_name="exifFocalPlaneXResolution",
        ),
        Column(
            "dam_special_instructions", "VARCHAR", source_name="damSpecialInstructions"
        ),
        Column("height_feet", "NUMBER", source_name="HeightFeet"),
        Column("dam_bits_per_pixel", "VARCHAR", source_name="damBitsperpixel"),
        Column("exif_iso_speed_ratings", "VARCHAR", source_name="exifISOSpeedRatings"),
        Column("exif_file_source", "VARCHAR", source_name="exifFileSource"),
        Column("exif_exif_version", "VARCHAR", source_name="exifExifVersion"),
        Column("ps_aux_lens", "VARCHAR", source_name="psAuxLens"),
        Column("photoshop_credit", "VARCHAR", source_name="photoshopCredit"),
        Column("xmp_mmdocument_id", "VARCHAR", source_name="xmpMMDocumentID"),
        Column("dcdate_xmp_array_type", "VARCHAR", source_name="dcdate_xmpArrayType"),
        Column(
            "exif_exsensitivity_type", "VARCHAR", source_name="exifEXSensitivityType"
        ),
        Column("dam_mimetype", "VARCHAR", source_name="damMIMEtype"),
        Column("tiff_orientation", "VARCHAR", source_name="tiffOrientation"),
        Column("exif_exlens_model", "VARCHAR", source_name="exifEXLensModel"),
        Column(
            "photoshop_document_ancestors",
            "VARCHAR",
            source_name="photoshopDocumentAncestors",
        ),
        Column("basesku", "VARCHAR", source_name="basesku"),
        Column(
            "exif_focal_plane_resolution_unit",
            "VARCHAR",
            source_name="exifFocalPlaneResolutionUnit",
        ),
        Column("ps_aux_firmware", "VARCHAR", source_name="psAuxFirmware"),
        Column(
            "dam_physical_width_in_dpi", "NUMBER", source_name="damPhysicalwidthindpi"
        ),
        Column("photoshop_source", "VARCHAR", source_name="photoshopSource"),
        Column(
            "dam_physical_height_in_dpi", "NUMBER", source_name="damPhysicalheightindpi"
        ),
        Column("exif_subject_distance", "VARCHAR", source_name="exifSubjectDistance"),
        Column("model_type", "VARCHAR", source_name="ModelType"),
        Column("tiff_resolution_unit", "VARCHAR", source_name="tiffResolutionUnit"),
        Column(
            "xmp_mmoriginal_document_id",
            "VARCHAR",
            source_name="xmpMMOriginalDocumentID",
        ),
        Column("model_type_name", "VARCHAR", source_name="ModelTypeName"),
        Column("bottom_size", "VARCHAR", source_name="BottomSize"),
        Column("photoshop_iccprofile", "VARCHAR", source_name="photoshopICCProfile"),
        Column("photoshop_color_mode", "NUMBER", source_name="photoshopColorMode"),
        Column("dam_numberofimages", "NUMBER", source_name="damNumberofimages"),
        Column("exif_exposure_mode", "VARCHAR", source_name="exifExposureMode"),
        Column("exif_exposure_time", "VARCHAR", source_name="exifExposureTime"),
        Column("tiff_xresolution", "VARCHAR", source_name="tiffXResolution"),
        Column("top_size", "VARCHAR", source_name="TopSize"),
        Column("launch_date", "TIMESTAMP_LTZ(3)", source_name="launchDate"),
        Column("exif_pixel_ydimension", "VARCHAR", source_name="exifPixelYDimension"),
        Column(
            "exif_exposure_bias_value", "VARCHAR", source_name="exifExposureBiasValue"
        ),
        Column("xmp_metadata_date", "TIMESTAMP_LTZ(3)", source_name="xmpMetadataDate"),
        Column(
            "exif_exbody_serial_number", "NUMBER", source_name="exifEXBodySerialNumber"
        ),
        Column("exif_pixel_xdimension", "VARCHAR", source_name="exifPixelXDimension"),
        Column("exif_aperture_value", "VARCHAR", source_name="exifApertureValue"),
        Column("exif_white_balance", "VARCHAR", source_name="exifWhiteBalance"),
        Column("cq_name", "VARCHAR", source_name="cqName"),
        Column("cq_tags", "VARIANT", source_name="cqTags"),
        Column("product_store", "VARCHAR", source_name="productStore"),
        Column(
            "exif_isospeed_ratings_xmp_array_type",
            "VARCHAR",
            source_name="exifISOSpeedRatings_xmpArrayType",
        ),
        Column(
            "dam_numberoftextualcomments",
            "VARCHAR",
            source_name="damNumberoftextualcomments",
        ),
        Column(
            "tiff_bits_per_sample_xmp_array_type",
            "VARCHAR",
            source_name="tiffBitsPerSample_xmpArrayType",
        ),
        Column("xmp_mminstance_id", "VARCHAR", source_name="xmpMMInstanceID"),
        Column("xmp_modify_date", "TIMESTAMP_LTZ(3)", source_name="xmpModifyDate"),
        Column("xmp_label", "VARCHAR", source_name="xmpLabel"),
        Column("exif_exposure_program", "VARCHAR", source_name="exifExposureProgram"),
        Column("tiff_bits_per_sample", "VARCHAR", source_name="tiffBitsPerSample"),
        Column("cq_parent_path", "VARCHAR", source_name="cqParentPath"),
        Column("xmp_create_date", "TIMESTAMP_LTZ(3)", source_name="xmpCreateDate"),
        Column("tiff_image_width", "NUMBER", source_name="tiffImageWidth"),
        Column("dc_date", "VARCHAR", source_name="dcDate"),
        Column(
            "exif_exlens_specification_xmp_array_type",
            "VARCHAR",
            source_name="exifEXLensSpecification_xmpArrayType",
        ),
        Column(
            "exif_date_time_original",
            "TIMESTAMP_LTZ(3)",
            source_name="exifDateTimeOriginal",
        ),
        Column(
            "dam_digital_creation_date", "VARCHAR", source_name="damDigitalCreationDate"
        ),
        Column(
            "photoshop_legacy_iptcdigest",
            "VARCHAR",
            source_name="photoshopLegacyIPTCDigest",
        ),
        Column("dam_sha_1", "VARCHAR", source_name="damSha1"),
        Column("dam_size", "NUMBER", source_name="damSize"),
        Column("tiff_model", "VARCHAR", source_name="tiffModel"),
        Column(
            "exif_focal_plane_yresolution",
            "VARCHAR",
            source_name="exifFocalPlaneYResolution",
        ),
        Column(
            "photoshop_document_ancestors_xmp_array_type",
            "VARCHAR",
            source_name="photoshopDocumentAncestors_xmpArrayType",
        ),
        Column("exif_metering_mode", "VARCHAR", source_name="exifMeteringMode"),
        Column(
            "ps_aux_lens_serial_number", "VARCHAR", source_name="psAuxLensSerialNumber"
        ),
        Column("bra_size", "VARCHAR", source_name="BraSize"),
        Column("stylename", "VARCHAR", source_name="stylename"),
        Column("cq_product_reference", "VARCHAR", source_name="cqProductReference"),
        Column("tiff_samples_per_pixel", "VARCHAR", source_name="tiffSamplesPerPixel"),
        Column("height_inches", "VARCHAR", source_name="HeightInches"),
        Column(
            "exif_scene_capture_type", "VARCHAR", source_name="exifSceneCaptureType"
        ),
        Column("dc_title", "VARCHAR", source_name="dcTitle"),
        Column(
            "exif_exrecommended_exposure_index",
            "VARCHAR",
            source_name="exifEXRecommendedExposureIndex",
        ),
        Column(
            "tiff_photometric_interpretation",
            "VARCHAR",
            source_name="tiffPhotometricInterpretation",
        ),
        Column(
            "ps_aux_flash_compensation", "VARCHAR", source_name="psAuxFlashCompensation"
        ),
        Column(
            "ps_aux_approximate_focus_distance",
            "VARCHAR",
            source_name="psAuxApproximateFocusDistance",
        ),
        Column("updated_at", "TIMESTAMP_LTZ(3)", source_name="updated_at"),
    ],
    version="v2",
)
