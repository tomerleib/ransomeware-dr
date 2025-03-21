from .shared_imports import (
    Optional,
    ComponentResource,
    ResourceOptions,
    storage,
    pulumi,
    os,
    zipfile
)

class StorageComponent(ComponentResource):
    """
    Create a storage bucket and upload a zip archive of the specified object path.
    Args:
        name: str, the name of the storage bucket
        object_path: str, the path to the object to upload
        location: str, the location of the storage bucket
        opts: Optional[ResourceOptions] = None,
    """
    def __init__(
        self,
        name: str,
        object_path: str,
        location: str,
        opts: Optional[ResourceOptions] = None,
    ):
        super().__init__('custom:components:storage', name, {}, opts)
        opts = ResourceOptions(parent=self)
        self.name = name
        self.object_path = object_path
        self.location = location

        # Create zip archive first
        zip_path = self.create_zip_archive()

        self.create_bucket()
        self.upload_archive(zip_path)
        self.register_component_outputs()

    def create_zip_archive(self):
        zip_path = f"{self.object_path}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(self.object_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, self.object_path)
                    zipf.write(file_path, arcname)
        return zip_path

    def create_bucket(self):
        self.bucket = storage.Bucket(
            self.name,
            location=self.location,
            opts=ResourceOptions(parent=self)
        )

    def upload_archive(self, zip_path):
        self.bucket_object = storage.BucketObject(
            f"{self.name}-archive",
            bucket=self.bucket.name,
            source=pulumi.FileAsset(zip_path),
            opts=ResourceOptions(parent=self)
        )

    def register_component_outputs(self):
        self.register_outputs(
            {
                "bucket_name": self.bucket.name,
                "bucket_object": self.bucket_object.name,
            }
        )
