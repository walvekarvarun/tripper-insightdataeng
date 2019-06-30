from flask_wtf import FlaskForm
import wtforms
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired

class SelectForm(FlaskForm):
	city = wtforms.SelectField(label='CITY', choices=('a','b'))
    #city = StringField('city', validators=[DataRequired()])
    interests = StringField('interests', validators=[DataRequired()])
    submit = SubmitField('Submit')