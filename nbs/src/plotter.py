import altair as alt


class Plotter:
    HEIGHT = 400
    WIDTH = 800

    @staticmethod
    def plot_timeline(df, x, x2, y, y2=None, color=None):
        chart = alt.Chart(df).mark_bar().encode(
            x=x,
            x2=x2,
            y=y,
            tooltip=list(df.columns),
        )

        if y2 is not None:
            chart = chart.encode(y2=y2)

        if color is not None:
            chart = chart.encode(color=color)
        return chart.properties(
            height=Plotter.HEIGHT,
            width=Plotter.WIDTH
        )
