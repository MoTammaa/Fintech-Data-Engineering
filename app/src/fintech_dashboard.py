from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import functions as fn

fintech_df: pd.DataFrame
fintech_df, lookup = fn.read_data('dashboard')

app = Dash()

# Question 1
loan_amount_across_grades = px.box(fintech_df, x='letter_grade', y='loan_amount')
loan_amount_across_grades.update_layout(xaxis_title='Letter Grade', yaxis_title='Loan Amount')

# Question 2

# Question 3

# Question 4
avg_loan_amount_states = px.bar(
    fintech_df.groupby('state')['loan_amount'].mean().reset_index(),
    x='state', y='loan_amount',
    color= 'loan_amount',
    color_continuous_scale=px.colors.sequential.Viridis
)
avg_loan_amount_states.update_layout(xaxis_title='State', yaxis_title='Average Loan Amount')


# Question 5
loan_grades_distribution = px.histogram(
    (fintech_df.value_counts('grade') / fintech_df.shape[0]).reset_index(),
      x='grade', y='count'
)
loan_grades_distribution.update_layout(xaxis_title='Grade', yaxis_title='Percentage Distrb.')










app.layout = html.Div([
    html.H1(children='Fintech Dashboard for Mo Tammaa', style={'textAlign':'center'}, className='title'),

    # Question 1
    html.Div(children=[
        html.H3(children=fn.QUESTIONS[0], className='graph-title'),
        dcc.Graph(id='loan_amount_across_grades', figure=loan_amount_across_grades, className='my-graph')
    ], className='graph-container', id='graphcont0'),

    # Question 2

    # Question 3

    # Question 4
    html.Div(children=[
        html.H3(children=fn.QUESTIONS[3], className='graph-title'),
        dcc.Graph(id='avg_loan_amount_states', figure=avg_loan_amount_states, className='my-graph')
    ], className='graph-container', id='graphcont3'),

    # Question 5
    html.Div(children=[
        html.H3(children=fn.QUESTIONS[4], className='graph-title'),
        dcc.Graph(id='loan_grades_distribution', figure=loan_grades_distribution, className='my-graph')
    ], className='graph-container', id='graphcont4')


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