from django.shortcuts import render
from . import models

# Create your views here.

def index(request):
    add = models.qcwytable(key='00000',title='New Company')
    add.save();
    context = {
        'title':'ShiJiaZhuang',
        'des':'My home',
        'score':'1.0',
        'new_title':add.title
    }
    #调配index界面
    return render(request,"index.html",context)
