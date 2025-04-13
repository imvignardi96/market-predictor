import keras
import keras_tuner

class Smart(keras_tuner.HyperModel):
    def __init__(self, input_shape):
        # Store input shape as an attribute of the model
        self.input_shape = input_shape
        
    def build(self, hp):
        model = keras.Sequential()
        model.add(keras.layers.InputLayer(input_shape=self.input_shape))

        # Number of layers
        n_layers = hp.Int('num_layers', 1, 3)
        int_activation = hp.Choice('activation', values=['relu', 'tanh', 'sigmoid'])
        
        # Optional recurrent dropout
        recurrent_dropout = hp.Float('recurrent_dropout', 0.0, 0.3, step=0.1)
        
        # Define bidirectional option
        is_bidirectional = hp.Boolean('is_bidirectional')
        
        for i in range(n_layers):
            units = hp.Int(f'units_{i}', 32, 256, step=32)
            return_seq = i < n_layers - 1

            lstm_layer = keras.layers.LSTM(
                units,
                activation=int_activation,
                return_sequences=return_seq,
                recurrent_dropout=recurrent_dropout
            )

            if is_bidirectional:
                model.add(keras.layers.Bidirectional(lstm_layer))
            else:
                model.add(lstm_layer)
                
        # Optional batch normalization
        if hp.Boolean('use_batchnorm'):
            model.add(keras.layers.BatchNormalization())
            
        # Optional dense layer before output
        if hp.Boolean('extra_dense'):
            dense_units = hp.Int('dense_units', 32, 128, step=32)
            model.add(keras.layers.Dense(dense_units, activation=int_activation))

        # Optional Dropout
        dropout_rate = hp.Float('dropout_rate', 0.0, 0.5, step=0.1)
        if dropout_rate > 0:
            model.add(keras.layers.Dropout(dropout_rate))

        # Optimizer + Learning rate
        optimizer_choice = hp.Choice('optimizer', ['adam', 'rmsprop'])
        learning_rate = hp.Float('learning_rate', 1e-4, 1e-2, sampling='log')

        if optimizer_choice == 'adam':
            optimizer = keras.optimizers.Adam(learning_rate=learning_rate)
        else:
            optimizer = keras.optimizers.RMSprop(learning_rate=learning_rate)

        # Loss function
        loss_choice = hp.Choice('loss', ['mse', 'mae', 'huber'])

        model.compile(optimizer=optimizer, loss=loss_choice, metrics=['mae', 'mape'])

        return model

    def fit(self, hp, model, X_train, y_train, X_val, y_val, callbacks=None, **kwargs):
        # Retrieve hyperparameters passed for batch size, epochs, and callbacks
        batch_size = hp.Int('batch_size', 32, 128, step=32)  # Default batch size range
        epochs = hp.Int('epochs', 50, 200)  # Default epochs range

        if callbacks is None:
            callbacks = []

        # Call the default Keras fit method with these dynamic parameters
        history = model.fit(
            X_train, y_train,
            batch_size=batch_size,
            epochs=epochs,
            validation_data=(X_val, y_val),
            callbacks=callbacks,  # Pass the callbacks
            **kwargs
        )

        return history
