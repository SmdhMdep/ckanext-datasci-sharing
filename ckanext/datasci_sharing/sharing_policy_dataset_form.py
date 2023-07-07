from ckan.plugins import toolkit

from .config import SHARE_INTERNALLY_FIELD


class SharingPolicyDatasetForm(toolkit.DefaultDatasetForm):
    def _write_package_schema(self, schema):
        schema.update({
            SHARE_INTERNALLY_FIELD: [
                toolkit.get_validator('boolean_validator'),
                toolkit.get_converter('convert_to_extras')
            ],
        })
        return schema

    def create_package_schema(self):
        schema = super().create_package_schema()
        return self._write_package_schema(schema)

    def update_package_schema(self):
        schema = super().update_package_schema()
        return self._write_package_schema(schema)

    def show_package_schema(self):
        schema = super().show_package_schema()
        schema.update({
            SHARE_INTERNALLY_FIELD: [
                toolkit.get_converter('convert_from_extras'),
                toolkit.get_converter('default')(False)
            ],
        })
        return schema

    def is_fallback(self):
        return True

    def package_types(self):
        return []
