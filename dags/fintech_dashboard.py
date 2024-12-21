from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import functions as fn
import numpy as np





def create_dashboard():
    app = Dash()
    fintech_df: pd.DataFrame
    fintech_df, lookup = fn.read_data('dashboard')

    # revert the log transformations, one hot encodings, etc.
    fintech_df = adjust_df_to_be_ready_for_viewing(fintech_df)

    # Question 1
    loan_amount_across_grades = px.box(fintech_df, x='letter_grade', y='loan_amount')
    loan_amount_across_grades.update_layout(xaxis_title='Letter Grade', yaxis_title='Loan Amount')

    # Question 2
    @app.callback( Output('loan_amount_vs_annual_inc_across_states', 'figure'), Input('loan_amount_vs_annual_inc_across_states-dropdown', 'value'))
    def update_loan_amount_vs_annual_inc_across_states(value):
        dff = fintech_df[fintech_df['state'] == value] if (not value) or value.lower() != 'all' else fintech_df
        return px.scatter(dff, x='loan_amount', y='annual_inc', color='loan_status')\
                         .update_layout(xaxis_title='Loan Amount', yaxis_title='Annual Income')
        

    # Question 3
    @app.callback(Output('n_loans_per_month', 'figure'), Input('n_loans_per_month-dropdown', 'value'))
    def update_n_loans_per_month(value):
        dff: pd.DataFrame = fintech_df if (not value) or str(value).lower() == 'all' else fintech_df[fintech_df['issue_year'] == int(value)] 
        dff = dff.groupby('month_number').size().reset_index(name='count').sort_values('month_number') 
        figure = px.line(dff, x='month_number', y='count')\
            .update_layout(xaxis_title='Issue Month', yaxis_title='Number of Loans') 
        return figure

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


    # ---------------------------------------------------------------------------------------
    # App layout
    app.layout = html.Div([
        html.H1(children='Fintech Dashboard for Mo Tammaa', style={'textAlign':'center'}, className='title'),

        html.Div(children=[
            html.Plaintext(children='This dashboard was created by Mohamed Tammaa, id 52-20136', className='footer'),
            html.Plaintext(children='Thank you for your help throughout the course', className='footer')
        ], className='footer-container'),


        # Question 1
        html.Div(children=[
            html.H3(children=fn.QUESTIONS[0], className='graph-title'),
            dcc.Graph(id='loan_amount_across_grades', figure=loan_amount_across_grades, className='my-graph')
        ], className='graph-container', id='graphcont0'),

        # Question 2
        html.Div(children=[
            html.H3(children=fn.QUESTIONS[1], className='graph-title'),
            dcc.Dropdown(
                options=[{'label': state, 'value': state} for state in ['All'] + fintech_df['state'].unique().tolist()],
                value='All', id='loan_amount_vs_annual_inc_across_states-dropdown', className='my-dropdown'
            ),
            dcc.Graph(id='loan_amount_vs_annual_inc_across_states', className='my-graph')
        ], className='graph-container', id='graphcont1'),

        # Question 3
        html.Div(children=[
            html.H3(children=fn.QUESTIONS[2], className='graph-title'),
            dcc.Dropdown(
                options=[{'label': year, 'value': year} for year in ['All'] + sorted(fintech_df['issue_year'].unique().tolist())],
                value='All', id='n_loans_per_month-dropdown', className='my-dropdown'
            ),
            dcc.Graph(id='n_loans_per_month', className='my-graph')
        ], className='graph-container', id='graphcont2'),

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

    app.run(debug=True, port=8050, host='0.0.0.0')


def adjust_df_to_be_ready_for_viewing(df):
    # revert the log transformation of the loan amount
    df['loan_amount'] = np.expm1(df['loan_amount'])
    # revert the log transformation of the annual income
    df['annual_inc'] = np.expm1(df['annual_inc'])
    # get back loan status from one hot encoding
    loan_status_columns = [c for c in df.columns if c.startswith('loan_status_')]
    df['loan_status'] = df[loan_status_columns].idxmax(axis=1).apply(lambda x: x.replace('loan_status_', '').replace('_', ' '))

    # add issue_year column
    df['issue_year'] = pd.to_datetime(df['issue_date']).dt.year
    # add issue_m_y column
    df['issue_m_y'] = pd.to_datetime(df['issue_date']).dt.to_period('M')
    df['issue_m_y'] = df['issue_m_y'].astype(str)
    return df

if __name__ == '__main__':
    create_dashboard()
#     app.run(debug=True, port=8051, host='0.0.0.0')

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