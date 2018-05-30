from django.db import models


# Create your models here.


class qcwytable(models.Model):
  # class Meta:
  #   db_table = 'qcwytable'
  key = models.CharField(max_length=100,primary_key=True)
  title = models.CharField(max_length=100)
  link = models.CharField(max_length=200)
  company = models.CharField(max_length=100)
  salary = models.CharField(max_length=20)
  updatetime = models.CharField(max_length=20)
  salary_range = models.CharField(max_length=30)
  num = models.CharField(max_length=10)
  parent_link = models.CharField(max_length=200)