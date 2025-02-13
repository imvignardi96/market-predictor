import matplotlib.pyplot as plt

class LSTMPlotter:
    def __init__(self, rows, cols):
        # Initialize the figure and axes for the grid of plots
        self.rows = rows
        self.cols = cols
        self.fig, self.axes = plt.subplots(rows, cols, figsize=(15, 12))
        self.axes = self.axes.flatten()  # Flatten the 2D axes array for easier indexing
        self.plot_idx = 0  # Keeps track of the current subplot index
    
    def add_plot(self, y_test:list, y_pred:list, model_name:str, metrics:dict = None):
        # Add a plot to the appropriate subplot
        if self.plot_idx < len(self.axes):
            ax = self.axes[self.plot_idx]
            ax.plot(y_pred, label='Predicted prices')
            ax.plot(y_test, label='Actual prices')
            ax.set_title(f'Model {model_name}')
            ax.set_xlabel('Index')
            ax.set_ylabel('Closing Price')
            
            if metrics is not None:
                metrics_text = ''
                for metric, value in metrics.items():
                    metrics_text+=f'{metric}: {value:.2f}\n'
                    
                ax.text(5, max(y_test) * 0.995, metrics_text, fontsize=18, color='black', 
                    bbox=dict(facecolor='white', alpha=0.6))
            
            self.plot_idx += 1  # Move to the next subplot
        else:
            print("Warning: No more space in the subplot grid!")
            
    def show(self):
        # Adjust layout and display the final plot
        plt.tight_layout()
        plt.show()
