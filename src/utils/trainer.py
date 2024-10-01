import torch


class Trainer:
    def __init__(self, model, optimizer, criterion, device):
        self.model = model
        self.optimizer = optimizer
        self.criterion = criterion
        self.device = device

    def preprocess(self, inputs, labels):
        inputs = torch.reshape(inputs, (-1, inputs.shape[-1]))
        labels = torch.reshape(labels, (-1, self.model.num_classes))
        return inputs, labels

    def train_step(self, inputs, labels):
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)
        labels_decoded = torch.argmax(labels, dim=1)

        self.optimizer.zero_grad()

        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels_decoded)
        loss.backward()
        self.optimizer.step()

        preds = torch.argmax(outputs, dim=1)

        return loss.item(), preds

    def test_step(self, inputs, labels):
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)
        labels_decoded = torch.argmax(labels, dim=1)

        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels_decoded)

        preds = torch.argmax(outputs, dim=1)

        return loss.item(), preds

    def train(self, train_loader, epoch, print_every=100):
        self.model.train()
        running_loss = 0.0
        for i, data in enumerate(train_loader, 0):
            inputs, labels = data
            loss, preds = self.train_step(inputs, labels)

            running_loss += loss
            if i % print_every == print_every - 1:
                print(
                    f"[{epoch + 1}, {i + 1:5d}] loss: {running_loss / print_every:.3f}"
                )
                running_loss = 0.0

    def test(self, test_loader):
        self.model.eval()
        running_loss = 0.0
        with torch.no_grad():
            for data in test_loader:
                inputs, labels = data
                loss, preds = self.test_step(inputs, labels)
                running_loss += loss

        return running_loss / len(test_loader)

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))
