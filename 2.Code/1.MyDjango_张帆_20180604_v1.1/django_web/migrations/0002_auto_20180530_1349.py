# Generated by Django 2.0.5 on 2018-05-30 13:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('django_web', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='qcwytable',
            fields=[
                ('key', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('title', models.CharField(max_length=100)),
                ('link', models.CharField(max_length=200)),
                ('company', models.CharField(max_length=100)),
                ('salary', models.CharField(max_length=20)),
                ('updatetime', models.CharField(max_length=20)),
                ('salary_range', models.CharField(max_length=30)),
                ('num', models.CharField(max_length=10)),
                ('parent_link', models.CharField(max_length=200)),
            ],
        ),
        migrations.DeleteModel(
            name='user',
        ),
    ]
