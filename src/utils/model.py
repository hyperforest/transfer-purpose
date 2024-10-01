import torch


class FCN(torch.nn.Module):
    def __init__(self, labels, input_shape, n_hiddens, dropout_rate=0.1):
        super().__init__()

        self.labels = labels
        self.num_classes = len(labels)
        self.fcns = torch.nn.ModuleList()
        self.dropouts = torch.nn.ModuleList()

        for i, n_hidden in enumerate(n_hiddens):
            if i == 0:
                self.fcns.append(
                    torch.nn.Linear(in_features=input_shape, out_features=n_hidden)
                )
            else:
                self.fcns.append(
                    torch.nn.Linear(in_features=n_hiddens[i - 1], out_features=n_hidden)
                )

            self.dropouts.append(torch.nn.Dropout(p=dropout_rate))

        self.head = torch.nn.Linear(in_features=n_hiddens[-1], out_features=self.num_classes)
        self.softmax = torch.nn.Softmax(dim=1)

    def forward(self, x):
        for i, fcn in enumerate(self.fcns):
            x = fcn(x)
            x = torch.nn.functional.silu(x)
            x = self.dropouts[i](x)

        x = self.head(x)
        x = self.softmax(x)
        return x
