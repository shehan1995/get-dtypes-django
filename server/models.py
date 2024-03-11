from django.db import models


class ProcessedFiles(models.Model):
    id = models.AutoField(primary_key=True)
    file_name = models.CharField(max_length=120)
    completed = models.BooleanField(default=False)
    data_types = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def _str_(self):
        return self.id


# class DTypes(models.Model):
#     file = models.ForeignKey(ProcessFiles, db_index=True)
#     name = models.CharField(max_length=240)
#     dtype = models.CharField(max_length=240)
