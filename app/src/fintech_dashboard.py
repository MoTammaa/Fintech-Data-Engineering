from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import functions as fn

fintech_df, lookup = fn.read_data('dashboard')

app = Dash()

loan_amount_across_grades = px.box(fintech_df, x='letter_grade', y='loan_amount')
loan_amount_across_grades.update_layout(title={'text':fn.QUESTIONS[0], 'x':0.5, 'xanchor':'center'},
                                        xaxis_title='Letter Grade', 
                                        yaxis_title='Loan Amount')










app.layout = html.Div([
    html.H1(children='Fintech Dashboard for Mo Tammaa', style={'textAlign':'center'}, className='title'),
    dcc.Graph(id='loan_amount-across-grades', figure=loan_amount_across_grades, className='my-graph')
], className='container')

if __name__ == '__main__':
    app.run(debug=True, port=8050, host='0.0.0.0')

# df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv')

# app = Dash()

# app.layout = html.Div([
#     html.H1(children='Title of Dash App', style={'textAlign':'center'}, className='title'),
#     dcc.Dropdown(df.country.unique(), 'Canada', id='dropdown-selection'),
#     dcc.Graph(id='graph-content')
# ], className='container')

# @callback(
#     Output('graph-content', 'figure'),
#     Input('dropdown-selection', 'value')
# )
# def update_graph(value):
#     dff = df[df.country==value]
#     return px.line(dff, x='year', y='pop')

# if __name__ == '__main__':
#     app.run(debug=True)