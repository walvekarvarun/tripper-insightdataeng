from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired

class SelectForm(FlaskForm):
    city = StringField('city', validators=[DataRequired()])
    interests = StringField('interests', validators=[DataRequired()])
    submit = SubmitField('Submit')