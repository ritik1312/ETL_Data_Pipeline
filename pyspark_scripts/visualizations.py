import plotly.express as px
from config.logging_config import logger
import utils.const_messages as message
from utils.exception import FileHandlingException, VisualizationFailureException


class Visualizer():

    def __init__(self, data):
        self.data_to_plot = data
        self.saved_plots_path = "./plots"
        logger.info(f"{message.OBJECT_INITIALIZATION} {self.__class__.__name__}.")


    def get_pie_plot(self, values, names, title):

        try:
            fig = px.pie(
                            self.data_to_plot,
                            values=values,
                            names=names,
                            title=title,
                            hover_data=[values]
                        )
            
            fig.update_layout(title_x=0.5)
            fig.show()
            logger.info(f"Pie chart titled {title} plotted successfully")

        except Exception as error:

            logger.error(f"Failed to plot pie chart titled: {title}. Error details: {str(error)}")
            raise VisualizationFailureException(f"Failed to plot pie chart titled: {title}")

        try:
            fig.write_image(f"{self.saved_plots_path}/{title}.jpg")
            logger.info(f"Pie chart titled {title} saved to {self.saved_plots_path}")

        except Exception as error:

            logger.error(f"Failed to save pie chart: {title}. Error details: {str(error)}")
            raise FileHandlingException(f"Failed to save pie chart: {title}")



    def get_bar_plot(self, x_col, y_col, title):

        try:
            fig = px.bar(
                            self.data_to_plot, 
                            x=x_col, 
                            y=y_col, 
                            labels={x_col: 'Age Group', y_col: 'Count'}, 
                            title=title,
                            hover_data=[y_col]
                        )
            
            fig.update_layout(title_x=0.5)
            fig.show()

            logger.info(f"Bar Graph titled {title} plotted successfully")

        except Exception as error:

            logger.error(f"Failed to plot bar graph titled: {title}. Error details: {str(error)}")
            raise VisualizationFailureException(f"Failed to plot bar graph: {title}")

        try:
            fig.write_image(f"{self.saved_plots_path}/{title}.jpg")
            logger.info(f"Bar Graph titled {title} saved to {self.saved_plots_path}")

        except Exception as error:

            logger.error(f"Failed to save bar plot: {title}. Error details: {str(error)}")
            raise FileHandlingException(f"Failed to save bar graph: {title}")




    def get_geo_map_plot(self, locations_name, color, title):
        
        try:
            fig = px.choropleth(
                                    self.data_to_plot,
                                    locations=locations_name,
                                    locationmode='country names',  # 'country names' or 'ISO-3' based on data
                                    color=color,
                                    hover_name=locations_name,
                                    color_continuous_scale=px.colors.sequential.Plasma
                                )


            fig.update_layout(
                                title_text=title,
                                title_x=0.5,

                                geo=dict(
                                    showframe=False,
                                    showcoastlines=False,
                                    projection_type='equirectangular'
                                ),

                                width=1100,
                                height = 600
                            )

            fig.show()
            logger.info(f"World map titled {title} plotted successfully")
        
        except Exception as error:

            logger.error(f"Failed to plot world map titled: {title}. Error details: {str(error)}")
            raise VisualizationFailureException(f"Failed to plot world map: {title}")

        try:
            fig.write_image(f"{self.saved_plots_path}/{title}.jpg")
            logger.info(f"World map titled {title} saved to {self.saved_plots_path}")

        except Exception as error:

            logger.error(f"Failed to save world map: {title}. Error details: {str(error)}")
            raise FileHandlingException(f"Failed to save world map: {title}")