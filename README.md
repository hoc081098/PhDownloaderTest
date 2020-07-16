# PhDownloaderTest
RxAlamofire + RxSwift Downloader

## Create downloader
```swift
private let downloader: PhDownloader = PhDownloaderFactory.makeDownloader(with: .init(
    maxConcurrent: 2,
    throttleProgress: .milliseconds(500))
)
```

## Obseve download result (show snackbar, toast, alert)
```swift
self.downloader
    .downloadResult$
    .subscribe(onNext: { result in
      switch result {
      case .success(let request):
        print("[Result] Success: id=\(request.identifier)")
      case .failure(let request, let error):
        print("[Result] Failure: id=\(request.identifier), error=\(error)")
      case .cancelled(let request):
        print("[Result] Cancelled: id=\(request.identifier)")
      }
    })
    .disposed(by: self.disposeBag)
```

## Obseve download state (for update UI)
```swift
self.downloader
    .observe(by: self.items.map { $0.request.identifier })
    .subscribe(onNext: { [weak self] tasks in
      guard let self = self else { return }
      
      let newItems: [Item] = self.items.map { item in
        var copy = item
        copy.state = tasks[item.request.identifier]?.state ?? .undefined
        return copy
      }
    
      let indexPaths = zip(self.items, newItems)
        .enumerated()
        .compactMap { (index, tuple) -> IndexPath? in
          let (old, new) = tuple
          if old.state != new.state { return IndexPath(row: index, section: 0) }
          return nil
        }
    
      self.items = newItems
      self.tableView.reloadRows(at: indexPaths, with: .none)
    })
    .disposed(by: self.disposeBag)
```

## Enqueue, cancel, cancelAll, remove:
```swift
let id = "Request id"

self.downloader
    .enqueue(
        .init(
            identifier: String(id),
            url: URL(string: "https://file-examples-com.github.io/uploads/2017/04/file_example_MP4_1920_18MG.mp4")!,
            fileName: "test_file_\(id).mp4",
            savedDir: FileManager.default
                .urls(for: .documentDirectory, in: .userDomainMask)
                .first!
                .appendingPathComponent("downloads", isDirectory: true)
         )
    )
    .subscribe(
        onCompleted: { print("[Enqueue] Success: id=\(id)") },
        onError: { print("[Enqueue] Failure: id=\(id), error=\($0)") }
    )
    .disposed(by: self.disposeBag)

self.downloader
    .cancel(by: id)
    .subscribe(
      onCompleted: { print("[Cancel] Success: id=\(id)") },
      onError: { print("[Cancel] Failure: id=\(id), error=\($0)") }
    )
    .disposed(by: self.disposeBag)

self.downloader
    .cancelAll()
    .subscribe()
    .disposed(by: self.disposeBag)
    
self.downloader
    .remove(identifier: id, deleteFile: true)
    .subscribe()
    .disposed(by: self.disposeBag)
```
