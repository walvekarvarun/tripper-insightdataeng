from flask import render_template, flash, redirect, url_for
from app import app
from app.forms import SelectForm
@app.route('/')
@app.route('/index')
def index():
    user = {'username': 'Tripper User'}
    return render_template('index.html', title='Home', user=user)
@app.route('/')
@app.route('/select' , methods = ['GET','POST'])
def select():
        form = SelectForm()
        return render_template('SelectForm.html', title='Select', form=form)