//
//  ViewController.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/4/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import UIKit
import RxSwift

struct Item {
  let request: PhDownloadRequest
  var state: PhDownloadState
}

class ViewController: UIViewController {

  @IBOutlet weak var tableView: UITableView!

  private let downloader: PhDownloader = try! PhDownloaderFactory.makeDownloader(with: .init(
    maxConcurrent: 10,
    throttleProgress: .milliseconds(200))
  )

  private let disposeBag = DisposeBag()

  private var items: [Item] = (0..<100).map { i in
      .init(
        request: .init(
          identifier: String(i),
          url: URL(string: "https://file-examples-com.github.io/uploads/2017/04/file_example_MP4_1920_18MG.mp4")!,
          fileName: "test_file_\(i)",
          savedDir: FileManager.default
            .urls(for: .documentDirectory, in: .userDomainMask)
            .first!
            .appendingPathComponent("downloads", isDirectory: true)
        ),
        state: .undefined
      )
  }

  override func viewDidLoad() {
    super.viewDidLoad()


//    var b = false
//    Observable.just(1).delay(.seconds(2), scheduler: MainScheduler.instance)
//    .do(onCompleted: { b = true })
//      .takeUntil(Observable<Int>.timer(.seconds(5), scheduler: MainScheduler.instance))
//      .do(
//        onCompleted: { print("onCompleted") },
//        afterCompleted: { print("afterCompleted") },
//        onDispose: { print("onDispose \(b)") }
//      )
//      .subscribe()
//    return

    self.tableView.dataSource = self
    self.tableView.delegate = self

    self.downloader
      .downloadResult$
      .subscribe(onNext: { result in
        switch result {
        case .success(let request):
          print("Success: \(request.identifier)")
        case .failure(let request, let error):
          print("Failure: \(request.identifier) - \(error)")
        case .cancelled(let request):
          print("Cancel: \(request.identifier)")
        }
      })
      .disposed(by: self.disposeBag)

    self.downloader
      .observeState(by: self.items.map { $0.request.identifier })
      .throttle(.milliseconds(500), latest: true, scheduler: MainScheduler.instance)
      .subscribe(onNext: { [weak self] states in
        guard let self = self else { return }

        let newItems: [Item] = self.items.map { item in
          var copy = item
          copy.state = states[item.request.identifier] ?? .undefined
          return copy
        }

        let indexPaths = zip(self.items, newItems)
          .enumerated()
          .compactMap { (index, tuple) -> IndexPath? in
            let (old, new) = tuple
            if old.state != new.state {
              return IndexPath(row: index, section: 0)
            }
            return nil
        }

        self.items = newItems
        self.tableView.reloadRows(at: indexPaths, with: .none)
      })
      .disposed(by: self.disposeBag)

    DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
      self.items.map { $0.request }.forEach { request in
        self.downloader
          .enqueue(request)
          .subscribe()
          .disposed(by: self.disposeBag)

      }
    }
  }
}

extension ViewController: UITableViewDataSource, UITableViewDelegate {
  func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
    items.count
  }

  func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
    let cell = tableView.dequeueReusableCell(withIdentifier: "DownloadCell", for: indexPath)
    let item = self.items[indexPath.row]

    cell.textLabel?.text = item.request.url.absoluteString
    cell.detailTextLabel?.text = "\(item.state)"
    cell.detailTextLabel?.textColor = color(for: item.state)

    return cell
  }

  func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
    tableView.deselectRow(at: indexPath, animated: true)
    self.downloader
      .cancel(by: self.items[indexPath.row].request.identifier)
      .subscribe()
      .disposed(by: self.disposeBag)
  }
}

func color(for state: PhDownloadState) -> UIColor {
  switch state {

  case .undefined:
    return .darkGray
  case .enqueued:
    return .orange
  case .downloading:
    return .green
  case .completed:
    return .blue
  case .failed:
    return .red
  case .cancelled:
    return .systemPink
  }
}
