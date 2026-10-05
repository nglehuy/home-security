output "images" {
  description = "The custom images that this root deploys."
  value = {
    flink = local.flink_image
    spark = local.spark_image
  }
}
