import torch
from tqdm import tqdm
from sklearn.metrics import f1_score


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
        self.model.train()
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)

        self.optimizer.zero_grad()
        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels)
        loss.backward()
        self.optimizer.step()

        preds = torch.argmax(outputs, dim=1)
        f1 = f1_score(torch.argmax(labels, dim=1), preds, average="macro")
        metrics = {
            "loss": loss.item(),
            "f1": f1,
        }

        return metrics, preds

    def test_step(self, inputs, labels):
        self.model.eval()
        inputs, labels = inputs.to(self.device), labels.to(self.device)
        inputs, labels = self.preprocess(inputs, labels)

        outputs = self.model(inputs)
        loss = self.criterion(outputs, labels)

        preds = torch.argmax(outputs, dim=1)
        f1 = f1_score(torch.argmax(labels, dim=1), preds, average="macro")
        metrics = {
            "loss": loss.item(),
            "f1": f1,
        }
        # print(preds)

        return metrics, preds

    def fit(self, train_loader, valid_loader, epochs, verbose=0):
        for epoch in range(epochs):
            text_prefix = f"[Train] [Epoch {epoch + 1}/{epochs}] "
            _ = self.train(
                train_loader, verbose=(verbose == 2), text_prefix=text_prefix
            )

            text_prefix = f"[Eval ] [Epoch {epoch + 1}/{epochs}] "
            train_metrics, train_preds = self.eval(
                train_loader,
                verbose=(verbose >= 1),
                text_prefix=text_prefix,
                metrics_prefix="",
            )

            text_prefix = f"[Eval ] [Epoch {epoch + 1}/{epochs}] "
            val_metrics, val_preds = self.eval(
                valid_loader,
                verbose=(verbose >= 1),
                text_prefix=text_prefix,
                metrics_prefix="val_",
            )

            if verbose >= 1:
                metrics = {**train_metrics, **val_metrics}
                text = " - ".join([f"{k}: {v:.4f}" for k, v in metrics.items()])
                print(f"[Epoch {epoch + 1}/{epochs}] {text}")

            if verbose > 0:
                print()

    def train(self, train_loader, verbose=True, text_prefix=None):
        text_prefix = text_prefix or ""
        pbar = train_loader

        running_loss = 0.0
        targets, predictions = [], []

        self.model.train()

        if verbose:
            pbar = tqdm(train_loader)

        for data in pbar:
            inputs, labels = data
            metrics, preds = self.train_step(inputs, labels)
            running_loss += metrics["loss"]

            labels = torch.reshape(labels, (-1, self.model.num_classes))
            targets.append(torch.argmax(labels, dim=1))
            predictions.append(preds)

            if verbose:
                text = f"{text_prefix}" + " ".join(
                    [f"{k}: {v:.4f}" for k, v in metrics.items()]
                )
                pbar.set_description_str(text)

        targets = torch.cat(targets)
        predictions = torch.cat(predictions)
        # print(targets.shape, predictions.shape)

        loss = running_loss / len(train_loader)
        f1 = f1_score(targets, predictions, average="macro")

        metrics = {
            "loss": loss,
            "f1": f1,
        }
        return metrics, predictions

    def eval(self, test_loader, verbose=True, text_prefix=None, metrics_prefix=None):
        text_prefix = text_prefix or ""
        metrics_prefix = metrics_prefix or ""

        running_loss = 0.0
        targets, predictions = [], []

        self.model.eval()

        pbar = test_loader
        if verbose:
            pbar = tqdm(test_loader)

        with torch.no_grad():
            for data in pbar:
                inputs, labels = data
                metrics, preds = self.test_step(inputs, labels)
                running_loss += metrics["loss"]

                labels = torch.reshape(labels, (-1, self.model.num_classes))
                targets.append(torch.argmax(labels, dim=1))
                predictions.append(preds)

                if verbose:
                    text = f"{text_prefix}" + " ".join(
                        [f"{metrics_prefix}{k}: {v:.4f}" for k, v in metrics.items()]
                    )
                    pbar.set_description_str(text)

        targets = torch.cat(targets)
        predictions = torch.cat(predictions)
        
        loss = running_loss / len(test_loader)
        f1 = f1_score(targets, predictions, average="macro")
        
        metrics = {
            f"{metrics_prefix}loss": loss,
            f"{metrics_prefix}f1": f1,
        }
        return metrics, predictions

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))
